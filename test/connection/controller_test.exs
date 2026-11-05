defmodule Klife.Connection.ControllerTest do
  use ExUnit.Case

  alias Klife.Connection.Controller, as: ConnController

  @brokers_key {:known_brokers_ids, MyClient}
  @controller_key {:cluster_controller, MyClient}

  setup do
    brokers = :persistent_term.get(@brokers_key)
    controller = :persistent_term.get(@controller_key)

    on_exit(fn ->
      # Make sure later tests see the cluster info even if a test fails midway
      if :persistent_term.get(@brokers_key, :not_found) == :not_found,
        do: :persistent_term.put(@brokers_key, brokers)

      if :persistent_term.get(@controller_key, :not_found) == :not_found,
        do: :persistent_term.put(@controller_key, controller)
    end)

    %{brokers: brokers, controller: controller}
  end

  # Simulates the initial metadata request failing while the controller starts,
  # which leaves the cluster info unset until the next successful check
  defp erase_cluster_info do
    :persistent_term.erase(@brokers_key)
    :persistent_term.erase(@controller_key)
  end

  test "get_known_brokers/1 waits for the cluster info when it is missing", ctx do
    erase_cluster_info()

    assert Enum.sort(ConnController.get_known_brokers(MyClient)) == Enum.sort(ctx.brokers)
  end

  test "get_random_broker_id/1 waits for the cluster info when it is missing", ctx do
    erase_cluster_info()

    assert ConnController.get_random_broker_id(MyClient) in ctx.brokers
  end

  test "get_cluster_controller/1 waits for the cluster info when it is missing", ctx do
    erase_cluster_info()

    # With KRaft the metadata response reports a random live broker as the
    # controller, so it may differ between checks
    assert ConnController.get_cluster_controller(MyClient) in ctx.brokers
  end

  test "concurrent callers all get a broker while the cluster info is missing", ctx do
    erase_cluster_info()

    results =
      1..20
      |> Enum.map(fn _ -> Task.async(fn -> ConnController.get_random_broker_id(MyClient) end) end)
      |> Task.await_many(:timer.seconds(15))

    assert Enum.all?(results, &(&1 in ctx.brokers))
  end

  test "requests to :any broker succeed while the cluster info is missing" do
    erase_cluster_info()

    assert {:ok, %{content: %{brokers: [_ | _]}}} =
             Klife.Connection.Broker.send_message(
               KlifeProtocol.Messages.Metadata,
               MyClient,
               :any,
               %{
                 topics: [],
                 allow_auto_topic_creation: false,
                 include_topic_authorized_operations: false
               }
             )
  end
end
