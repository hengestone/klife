defmodule Klife.Connection.SaslRejectionTest do
  use ExUnit.Case

  import Klife.ProcessRegistry, only: [registry_lookup: 1]

  alias Klife.Connection
  alias Klife.Connection.Broker
  alias Klife.Connection.Controller, as: ConnController
  alias KlifeProtocol.Messages.ApiVersions

  @moduletag capture_log: true

  # Brokers without a configured validator accept unsecured JWTs ("alg": "none")
  # and refuse expired ones with {"status":"invalid_token"}
  defp unsecured_jwt(expires_in_seconds) do
    exp = System.os_time(:second) + expires_in_seconds
    header = Base.url_encode64(~s({"alg":"none"}), padding: false)
    claims = Base.url_encode64(~s({"sub":"klifeusr","exp":#{exp}}), padding: false)
    "#{header}.#{claims}."
  end

  # Stands for a token cache: `:valid`, `:refused`, or `:refused_once`, refused for the
  # first authentication only, as if `on_rejected_mfa` dropped it from the cache
  def token(source) do
    case Agent.get_and_update(source, fn
           :refused_once -> {:refused, :valid}
           mode -> {mode, mode}
         end) do
      :valid -> unsecured_jwt(60)
      :refused -> unsecured_jwt(-60)
    end
  end

  def token_rejected(test_pid, token, error), do: send(test_pid, {:token_rejected, token, error})

  defp start_client(client_name, token_mode) do
    source = :"#{client_name}.token_source"

    start_supervised!(%{
      id: :token_source,
      start: {Agent, :start_link, [fn -> token_mode end, [name: source]]}
    })

    input = %{
      client_name: client_name,
      bootstrap_servers: ["localhost:19094", "localhost:29094"],
      ssl: true,
      connect_opts: [
        verify: :verify_peer,
        cacertfile: Path.relative("test/compose_files/ssl/ca.crt")
      ],
      socket_opts: [],
      sasl_opts: [
        mechanism: "OAUTHBEARER",
        mechanism_opts: [
          token_mfa: {__MODULE__, :token, [source]},
          on_rejected_mfa: {__MODULE__, :token_rejected, [self()]}
        ]
      ],
      connection_count: 1,
      default_request_timeout_ms: 1_000
    }

    {start_supervised({ConnController, input}), source}
  end

  defp set_tokens(source, mode), do: Agent.update(source, fn _ -> mode end)

  defp broker_pid(client_name, broker_id) do
    [{pid, _}] = registry_lookup({Broker, broker_id, client_name, 0})
    pid
  end

  defp broker_conn(client_name, broker_id),
    do: :persistent_term.get({Broker, client_name, broker_id, 0})

  defp assert_broker_works(client_name, broker_id) do
    assert {:ok, %{content: %{api_keys: [_ | _]}}} =
             Broker.send_message(ApiVersions, client_name, broker_id)
  end

  # Retries are 0.5-1.5s apart at first
  defp wait_until(fun, attempts \\ 100) do
    cond do
      fun.() ->
        :ok

      attempts == 0 ->
        flunk("condition not met in time")

      true ->
        Process.sleep(100)
        wait_until(fun, attempts - 1)
    end
  end

  test "fails to start when the token is refused, after notifying the cache", %{test: name} do
    assert {{:error, reason}, _source} = start_client(name, :refused)
    assert inspect(reason) =~ "sasl_authentication_failed"

    assert_received {:token_rejected, token, error}
    assert [_header, _claims, ""] = String.split(token, ".")
    assert error =~ "invalid_token"
  end

  test "a broker connection keeps the current one and retries re-authentication when the token is refused",
       %{test: name} do
    {{:ok, _pid}, source} = start_client(name, :valid)
    [broker_id | _] = ConnController.get_known_brokers(name)
    pid = broker_pid(name, broker_id)
    %Connection{socket: old_socket} = broker_conn(name, broker_id)

    set_tokens(source, :refused)
    send(pid, {:reauth, old_socket})

    assert_receive {:token_rejected, _token, _error}, 5_000
    assert Process.alive?(pid)
    assert %Connection{socket: ^old_socket} = broker_conn(name, broker_id)
    assert_broker_works(name, broker_id)

    # The retry uses a token from the cache again
    set_tokens(source, :valid)
    wait_until(fn -> broker_conn(name, broker_id).socket != old_socket end)

    assert pid == broker_pid(name, broker_id)
    assert_broker_works(name, broker_id)
  end

  test "a broker connection closed by the broker reconnects with backoff when the token is refused",
       %{test: name} do
    {{:ok, _pid}, source} = start_client(name, :valid)
    [broker_id | _] = ConnController.get_known_brokers(name)
    pid = broker_pid(name, broker_id)
    %Connection{socket: old_socket} = broker_conn(name, broker_id)

    set_tokens(source, :refused)
    # As if the broker closed the connection, e.g. its session ended
    send(pid, {:ssl_closed, old_socket})

    assert_receive {:token_rejected, _token, _error}, 5_000
    assert Process.alive?(pid)

    set_tokens(source, :valid)
    wait_until(fn -> broker_conn(name, broker_id).socket != old_socket end)

    assert pid == broker_pid(name, broker_id)
    assert_broker_works(name, broker_id)
  end

  test "the bootstrap connection keeps the current one and retries re-authentication when the token is refused",
       %{test: name} do
    {{:ok, pid}, source} = start_client(name, :valid)
    %Connection{socket: old_socket} = :sys.get_state(pid).bootstrap_conn

    set_tokens(source, :refused)
    send(pid, {:reauth_bootstrap_conn, old_socket})

    # Refused by both bootstrap servers
    assert_receive {:token_rejected, _token, _error}, 5_000
    assert_receive {:token_rejected, _token, _error}, 5_000
    assert Process.alive?(pid)
    assert %Connection{socket: ^old_socket} = :sys.get_state(pid).bootstrap_conn

    set_tokens(source, :valid)
    wait_until(fn -> :sys.get_state(pid).bootstrap_conn.socket != old_socket end)
    assert :sys.get_state(pid).bootstrap_attempts == 0
  end

  test "initializing the bootstrap connection again retries when the token is refused",
       %{test: name} do
    {{:ok, pid}, source} = start_client(name, :valid)
    %Connection{socket: old_socket} = :sys.get_state(pid).bootstrap_conn

    set_tokens(source, :refused)
    # As after a failed cluster check
    send(pid, :init_bootstrap_conn)

    assert_receive {:token_rejected, _token, _error}, 5_000
    assert Process.alive?(pid)

    set_tokens(source, :valid)
    wait_until(fn -> :sys.get_state(pid).bootstrap_conn.socket != old_socket end)
    assert ConnController.get_known_brokers(name) != []
  end

  test "a token refused by one bootstrap server moves on to the next one", %{test: name} do
    {{:ok, pid}, source} = start_client(name, :valid)
    %Connection{socket: old_socket, port: 19094} = :sys.get_state(pid).bootstrap_conn

    set_tokens(source, :refused_once)
    send(pid, {:reauth_bootstrap_conn, old_socket})

    assert_receive {:token_rejected, _token, _error}, 5_000
    assert %Connection{port: 29094} = :sys.get_state(pid).bootstrap_conn
  end
end
