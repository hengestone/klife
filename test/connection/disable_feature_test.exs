defmodule Klife.Connection.DisableFeatureTest do
  use ExUnit.Case

  import ExUnit.CaptureLog

  alias Klife.Connection.Broker
  alias Klife.Connection.Controller, as: ConnController
  alias Klife.Connection.MessageVersions, as: MV
  alias KlifeProtocol.Messages, as: M

  # Every test uses its own client name, so the flags it sets can't leak into
  # MyClient or other tests
  setup %{test: test_name} do
    client_name = :"#{__MODULE__}.#{test_name}"

    on_exit(fn ->
      for {{ConnController, ^client_name, _feature, :disabled?} = key, _} <-
            :persistent_term.get(),
          do: :persistent_term.erase(key)

      for {{:api_version, _mod, ^client_name} = key, _} <- :persistent_term.get(),
          do: :persistent_term.erase(key)
    end)

    %{client_name: client_name}
  end

  defp disabled_features(client_name) do
    for {{ConnController, ^client_name, feature, :disabled?}, true} <- :persistent_term.get(),
        do: feature
  end

  # The versions a real broker supports, as `MessageVersions.setup_versions/2` gets them
  defp server_versions do
    {:ok, %{content: %{api_keys: api_keys}}} = Broker.send_message(M.ApiVersions, MyClient, :any)
    Map.new(api_keys, &{&1.api_key, %{min: &1.min_version, max: &1.max_version}})
  end

  describe "disable_feature/2 fallback" do
    for feature <- [:testing, :simulator] do
      test "disables #{inspect(feature)}, which has no clause of its own", ctx do
        log =
          capture_log(fn ->
            assert :ok = ConnController.disable_feature(unquote(feature), ctx.client_name)
          end)

        assert ConnController.disabled_feature?(ctx.client_name, unquote(feature))
        assert log =~ "Feature #{unquote(feature)} disabled"
        assert log =~ inspect(ctx.client_name)
      end
    end

    test "disables a feature it does not know about", ctx do
      capture_log(fn ->
        assert :ok = ConnController.disable_feature(:some_future_feature, ctx.client_name)
      end)

      assert ConnController.disabled_feature?(ctx.client_name, :some_future_feature)
    end

    test "only disables the given feature on the given client", ctx do
      capture_log(fn -> ConnController.disable_feature(:simulator, ctx.client_name) end)

      assert disabled_features(ctx.client_name) == [:simulator]
      refute ConnController.disabled_feature?(ctx.client_name, :testing)
      refute ConnController.disabled_feature?(MyClient, :simulator)
    end

    test "does not replace the specific clauses", ctx do
      # :connection is required, so it still raises instead of being disabled
      assert_raise RuntimeError, ~r/required message for the connection system/, fn ->
        ConnController.disable_feature(:connection, ctx.client_name)
      end

      refute ConnController.disabled_feature?(ctx.client_name, :connection)

      log = capture_log(fn -> ConnController.disable_feature(:producer, ctx.client_name) end)
      assert log =~ "Producer feature disabled due to API version negotiation failure"
    end
  end

  describe "API version negotiation" do
    test "disables :simulator when the broker does not support CreatePartitions", ctx do
      server_data = Map.delete(server_versions(), M.CreatePartitions.api_key())

      log = capture_log(fn -> assert :ok = MV.setup_versions(server_data, ctx.client_name) end)

      assert disabled_features(ctx.client_name) == [:simulator]
      assert log =~ "Feature simulator disabled"
    end

    test "disables :testing and :consumer_group when no ListOffsets version is shared", ctx do
      server_data =
        Map.update!(server_versions(), M.ListOffsets.api_key(), fn _ -> %{min: 100, max: 100} end)

      capture_log(fn -> assert :ok = MV.setup_versions(server_data, ctx.client_name) end)

      assert Enum.sort(disabled_features(ctx.client_name)) == [:consumer_group, :testing]
    end

    test "keeps negotiating the remaining messages after disabling a feature", ctx do
      server_data = Map.delete(server_versions(), M.CreatePartitions.api_key())

      capture_log(fn -> MV.setup_versions(server_data, ctx.client_name) end)

      assert MV.get(ctx.client_name, M.Produce) == MV.get(MyClient, M.Produce)
      assert MV.get(ctx.client_name, M.Fetch) == MV.get(MyClient, M.Fetch)
    end
  end
end
