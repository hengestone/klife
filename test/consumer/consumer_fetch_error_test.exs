defmodule Klife.Consumer.ConsumerFetchErrorTest do
  use ExUnit.Case

  import ExUnit.CaptureLog

  alias Klife.Consumer.ConsumerGroup.Consumer
  alias Klife.Consumer.ConsumerGroup.TopicConfig

  @topic "fetch_error_test_topic"
  @partition 3
  @offset 42
  @fetch_interval_ms 50

  @socket_errors [:closed, :enotconn, :econnreset, :econnrefused, :epipe, :etimedout]

  defp build_state(overrides \\ []) do
    struct!(
      %Consumer{
        client_name: MyClient,
        cg_name: "fetch-error-test-group",
        topic_name: @topic,
        partition_idx: @partition,
        topic_config: %TopicConfig{fetch_interval_ms: @fetch_interval_ms},
        fetch_ref: make_ref(),
        fetch_batcher_monitor_ref: nil,
        idle_timeout: :infinity
      },
      overrides
    )
  end

  defp tpo, do: {@topic, @partition, @offset}

  describe "handle_fetch_error!/3 with socket errors" do
    for error <- @socket_errors do
      test "retries on #{inspect(error)}" do
        state = build_state()

        {new_state, log} =
          with_log(fn -> Consumer.handle_fetch_error!(state, unquote(error), tpo()) end)

        assert new_state.fetch_ref == nil
        assert log =~ "Fetch error on #{@topic}:#{@partition} at offset #{@offset}, retrying"
        assert log =~ inspect(unquote(error))

        # The retry is scheduled with jitter in 0..fetch_interval_ms
        assert_receive :poll_records, @fetch_interval_ms + 100
      end
    end
  end

  test "keeps the rest of the state untouched when retrying :closed" do
    state = build_state(latest_fetched_offset: @offset, empty_fetch_results_count: 7)

    new_state = Consumer.handle_fetch_error!(state, :closed, tpo())

    assert %Consumer{state | fetch_ref: nil} == new_state
  end

  test "schedules exactly one poll per retried error" do
    Consumer.handle_fetch_error!(build_state(), :closed, tpo())

    assert_receive :poll_records, @fetch_interval_ms + 100
    refute_receive :poll_records, @fetch_interval_ms + 100
  end

  test "still raises on unknown errors" do
    assert_raise RuntimeError, ~r/Unexpected fetch error.*error_code=:some_unknown_error/, fn ->
      Consumer.handle_fetch_error!(build_state(), :some_unknown_error, tpo())
    end
  end

  describe "handle_info/2 with a :closed fetch response" do
    test "does not crash and schedules a retry" do
      state = build_state()
      msg = {:klife_fetch_response, tpo(), {:error, :closed}}

      assert {:noreply, %Consumer{fetch_ref: nil}, :infinity} = Consumer.handle_info(msg, state)
      assert_receive :poll_records, @fetch_interval_ms + 100
    end

    test "demonitors the fetch batcher before retrying" do
      batcher = spawn(fn -> Process.sleep(:infinity) end)
      monitor_ref = Process.monitor(batcher)
      state = build_state(fetch_batcher_monitor_ref: monitor_ref)
      msg = {:klife_fetch_response, tpo(), {:error, :closed}}

      assert {:noreply, %Consumer{fetch_batcher_monitor_ref: nil}, _} =
               Consumer.handle_info(msg, state)

      Process.exit(batcher, :kill)
      refute_receive {:DOWN, ^monitor_ref, :process, _, _}, 100
    end
  end
end
