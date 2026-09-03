defmodule Membrane.RTSP.SourceTest do
  use ExUnit.Case, async: true

  import Membrane.ChildrenSpec
  import Membrane.Testing.Assertions

  alias Membrane.RTSP

  @moduletag :tmp_dir

  defmodule NotifyingSink do
    @moduledoc false
    use Membrane.Sink

    def_input_pad :input, accepted_format: _any
    def_options id: [spec: term()]

    @impl true
    def handle_init(_ctx, opts), do: {[], %{id: opts.id}}

    @impl true
    def handle_buffer(:input, buffer, _ctx, state),
      do: {[notify_parent: {:buffer, state.id, buffer.pts}], state}
  end

  defmodule TestPipeline do
    @moduledoc false

    use Membrane.Pipeline

    @spec start(Keyword.t() | map()) :: GenServer.on_start()
    def start(options) do
      Pipeline.start(__MODULE__, options)
    end

    @impl true
    def handle_init(_ctx, opts) do
      spec =
        child(:source, %Membrane.RTSP.Source{
          transport: opts[:transport] || :tcp,
          allowed_media_types: opts[:allowed_media_types] || [:video, :audio, :application],
          stream_uri: "rtsp://localhost:#{opts[:port]}/",
          timeout: opts[:timeout] || Membrane.Time.seconds(15),
          keep_alive_interval: opts[:keep_alive_interval] || Membrane.Time.seconds(15),
          on_connection_closed: opts[:on_connection_closed] || :send_eos,
          reconnect_attempts: opts[:reconnect_attempts] || 0,
          reconnect_delay: opts[:reconnect_delay] || Membrane.Time.seconds(1)
        })

      {[spec: spec], %{dest_folder: opts[:dest_folder], sink: opts[:sink] || :file}}
    end

    @impl true
    def handle_child_notification(
          {:set_up_tracks, tracks},
          _element,
          _ctx,
          %{sink: :notify} = state
        ) do
      spec =
        Enum.map(tracks, fn track ->
          get_child(:source)
          |> via_out(Pad.ref(:output, track.control_path))
          |> child({:sink, track.control_path}, %Membrane.RTSP.SourceTest.NotifyingSink{
            id: track.control_path
          })
        end)

      {[spec: spec], state}
    end

    @impl true
    def handle_child_notification({:set_up_tracks, tracks}, _element, _ctx, state) do
      spec =
        Enum.map(tracks, fn track ->
          file_name =
            case track.rtpmap.encoding do
              "H264" -> "out.h264"
              "H265" -> "out.hevc"
              "plain" -> "out.txt"
            end

          get_child(:source)
          |> via_out(Pad.ref(:output, track.control_path))
          |> child({:sink, track.control_path}, %Membrane.File.Sink{
            location: Path.join(state.dest_folder, file_name)
          })
        end)

      {[spec: spec], state}
    end

    @impl true
    def handle_child_notification(_message, _element, _ctx, state) do
      {[], state}
    end

    @impl true
    def handle_child_pad_removed(_child, _pad, _ctx, state) do
      {[], state}
    end
  end

  setup_all do
    {:ok, server} =
      RTSP.Server.start_link(
        port: 0,
        handler: Membrane.RTSP.RequestHandler,
        udp_rtp_port: 0,
        udp_rtcp_port: 0
      )

    {:ok, port} = RTSP.Server.port_number(server)

    %{server_port: port}
  end

  test "stream all media using tcp", %{server_port: port, tmp_dir: tmp_dir} do
    options = [
      module: TestPipeline,
      custom_args: %{port: port, dest_folder: tmp_dir}
    ]

    pid = Membrane.Testing.Pipeline.start_link_supervised!(options)

    assert_pipeline_notified(pid, :source, {:set_up_tracks, tracks})

    assert [
             %{type: :video, rtpmap: %{encoding: "H264"}},
             %{type: :video, rtpmap: %{encoding: "H265"}},
             %{type: :application, rtpmap: %{encoding: "plain"}}
           ] = Enum.sort_by(tracks, fn %{rtpmap: %{encoding: encoding}} -> encoding end)

    assert_end_of_stream(pid, {:sink, _ref}, :input, 5_000)
    assert_end_of_stream(pid, {:sink, _ref}, :input, 5_000)
    assert_end_of_stream(pid, {:sink, _ref}, :input, 5_000)

    :ok = Membrane.Testing.Pipeline.terminate(pid)

    assert File.exists?(Path.join(tmp_dir, "out.h264"))
    assert File.exists?(Path.join(tmp_dir, "out.hevc"))
    assert File.exists?(Path.join(tmp_dir, "out.txt"))

    assert File.read!(Path.join(tmp_dir, "out.h264")) == File.read!("test/fixtures/in.h264"),
           "content is not the same"

    assert File.read!(Path.join(tmp_dir, "out.hevc")) == File.read!("test/fixtures/in.hevc"),
           "content is not the same"

    assert File.read!(Path.join(tmp_dir, "out.txt")) == File.read!("test/fixtures/in.txt"),
           "content is not the same"
  end

  test "sends TEARDOWN when the pipeline is terminated", %{server_port: port, tmp_dir: tmp_dir} do
    Process.register(self(), :rtsp_teardown_listener)

    pid =
      Membrane.Testing.Pipeline.start_link_supervised!(
        module: TestPipeline,
        custom_args: %{port: port, dest_folder: tmp_dir}
      )

    assert_pipeline_notified(pid, :source, {:set_up_tracks, _tracks})
    # The server only reports TEARDOWN once PLAY has gone through
    assert_start_of_stream(pid, {:sink, _ref}, :input, 5_000)
    :ok = Membrane.Testing.Pipeline.terminate(pid)

    assert_receive :teardown, 2_000
  end

  test "re-establishes the session in place after the server drops the connection",
       %{server_port: port} do
    Process.register(self(), :rtsp_conn_listener)

    pid =
      Membrane.Testing.Pipeline.start_link_supervised!(
        module: TestPipeline,
        custom_args: %{
          port: port,
          sink: :notify,
          allowed_media_types: [:video],
          on_connection_closed: :raise_error,
          reconnect_attempts: 3,
          reconnect_delay: Membrane.Time.milliseconds(200)
        }
      )

    assert_receive {:rtsp_conn, camera_socket}, 5_000
    assert_pipeline_notified(pid, :source, {:set_up_tracks, _tracks})
    assert_pipeline_notified(pid, {:sink, "/in.h264"}, {:buffer, "/in.h264", pts_before}, 5_000)
    source_pid = Membrane.Testing.Pipeline.get_child_pid!(pid, :source)

    # The "camera" drops the connection mid-stream
    :ok = :gen_tcp.close(camera_socket)

    assert_receive {:rtsp_conn, _new_socket}, 5_000
    assert Process.alive?(pid)
    assert Membrane.Testing.Pipeline.get_child_pid!(pid, :source) == source_pid

    # No second set_up_tracks: downstream keeps its pads and links
    refute_pipeline_notified(pid, :source, {:set_up_tracks, _}, 500)

    # Drain what was in flight, then media resumes with monotonic timestamps
    pts_after = wait_for_pts_above(pid, pts_before, 10_000)
    assert pts_after > pts_before

    :ok = Membrane.Testing.Pipeline.terminate(pid)
  end

  defp wait_for_pts_above(pid, threshold, timeout) do
    assert_pipeline_notified(pid, {:sink, "/in.h264"}, {:buffer, "/in.h264", pts}, timeout)
    if pts > threshold, do: pts, else: wait_for_pts_above(pid, threshold, timeout)
  end

  test "stream specific media using tcp", %{server_port: port, tmp_dir: tmp_dir} do
    options = [
      module: TestPipeline,
      custom_args: %{
        port: port,
        dest_folder: tmp_dir,
        allowed_media_types: [:application]
      }
    ]

    pid = Membrane.Testing.Pipeline.start_link_supervised!(options)

    assert_end_of_stream(pid, {:sink, _ref}, :input, 5_000)

    :ok = Membrane.Testing.Pipeline.terminate(pid)

    assert File.exists?(Path.join(tmp_dir, "out.txt"))
    refute File.exists?(Path.join(tmp_dir, "out.h264"))
    refute File.exists?(Path.join(tmp_dir, "out.hevc"))

    assert File.read!(Path.join(tmp_dir, "out.txt")) == File.read!("test/fixtures/in.txt"),
           "content is not the same"
  end

  test "stream all media using udp", %{server_port: port, tmp_dir: tmp_dir} do
    options = [
      module: TestPipeline,
      custom_args: %{
        port: port,
        dest_folder: tmp_dir,
        transport: {:udp, 20_000, 20_020},
        timeout: Membrane.Time.seconds(1),
        keep_alive_interval: Membrane.Time.seconds(10)
      }
    ]

    {:ok, blocking_socket1} = :gen_udp.open(20_000)
    {:ok, blocking_socket2} = :gen_udp.open(20_003)

    pid = Membrane.Testing.Pipeline.start_link_supervised!(options)

    assert_pipeline_notified(pid, :source, {:set_up_tracks, tracks})

    assert [
             %{type: :video, rtpmap: %{encoding: "H264"}},
             %{type: :video, rtpmap: %{encoding: "H265"}},
             %{type: :application, rtpmap: %{encoding: "plain"}}
           ] = Enum.sort_by(tracks, fn %{rtpmap: %{encoding: encoding}} -> encoding end)

    assert_end_of_stream(pid, {:sink, _ref}, :input, 5_000)
    assert_end_of_stream(pid, {:sink, _ref}, :input, 5_000)
    assert_end_of_stream(pid, {:sink, _ref}, :input, 5_000)

    :ok = Membrane.Testing.Pipeline.terminate(pid)

    :gen_udp.close(blocking_socket1)
    :gen_udp.close(blocking_socket2)

    assert File.exists?(Path.join(tmp_dir, "out.h264"))
    assert File.exists?(Path.join(tmp_dir, "out.hevc"))
    assert File.exists?(Path.join(tmp_dir, "out.txt"))

    assert File.read!("test/fixtures/in.h264")
           |> String.starts_with?(File.read!(Path.join(tmp_dir, "out.h264"))),
           "content is not the same"

    assert File.read!("test/fixtures/in.hevc")
           |> String.starts_with?(File.read!(Path.join(tmp_dir, "out.hevc"))),
           "content is not the same"

    assert File.read!("test/fixtures/in.txt")
           |> String.starts_with?(File.read!(Path.join(tmp_dir, "out.txt"))),
           "content is not the same"
  end
end
