defmodule Klife.ClientTest do
  use ExUnit.Case, async: false

  # The examples fetch records produced right before with read committed, which
  # misses them while another example's transaction is still being finalized
  setup do
    Klife.TestUtils.wait_for_stable_topics(MyClient, ["my_topic_1", "my_topic_2", "my_topic_3"])
  end

  @tag capture_log: true
  doctest Klife.Client
end
