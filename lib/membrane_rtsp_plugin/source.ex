defmodule Membrane.RTSP.Source do
  @moduledoc """
  Source bin responsible for connecting to an RTSP server.

  This element connects to an RTSP server, depayloads and parses the received media if possible.
  If there's no suitable depayloader and parser, the raw payload is sent to the subsequent elements in
  the pipeline.

  In case connection can't be established this bin will crash. If the connection is severed
  during streaming the bin crashes too, unless `reconnect_attempts` is set, in which case the
  RTSP session is re-established in place and downstream keeps receiving the same tracks.

  The following codecs are depayloaded and parsed:
    * `H264`
    * `H265`
    * `AAC` (if sent according to RFC3640)
    * `Opus`

  When the element finishes setting up all tracks it will send a `t:set_up_tracks/0` notification.
  To receive a track a corresponding `Pad.ref(:output, control_path)` pad has to be connected,
  where each track's control path is provided in the `t:set_up_tracks/0` notification.
  """

  use Membrane.Bin

  require Membrane.Logger

  alias __MODULE__
  alias __MODULE__.ConnectionManager
  alias Membrane.{RTSP, Time}

  @type set_up_tracks_notification :: {:set_up_tracks, [track()]}
  @type track :: %{
          control_path: String.t(),
          type: :video | :audio | :application,
          framerate: ExSDP.Attribute.framerate_value() | nil,
          fmtp: ExSDP.Attribute.FMTP.t() | nil,
          rtpmap: ExSDP.Attribute.RTPMapping.t() | nil
        }

  @type transport ::
          {:udp, port_range_start :: non_neg_integer(), port_range_end :: non_neg_integer()}
          | :tcp

  def_options stream_uri: [
                spec: binary(),
                description: "The RTSP URI of the resource to stream."
              ],
              allowed_media_types: [
                spec: [:video | :audio | :application],
                default: [:video, :audio, :application],
                description: """
                The media type to accept from the RTSP server.
                """
              ],
              transport: [
                spec: transport(),
                default: :tcp,
                description: """
                Transport protocol that will be used in the established RTSP stream. In case of
                UDP a range needs to be provided from which receiving ports will be chosen.
                """
              ],
              timeout: [
                spec: Time.t(),
                default: Time.seconds(15),
                default_inspector: &Time.pretty_duration/1,
                description: "RTSP response timeout"
              ],
              keep_alive_interval: [
                spec: Time.t(),
                default: Time.seconds(15),
                default_inspector: &Time.pretty_duration/1,
                description: """
                Interval of a heartbeat sent to the RTSP server at a regular interval to
                keep the session alive.
                """
              ],
              on_connection_closed: [
                spec: :raise_error | :send_eos,
                default: :raise_error,
                description: """
                Defines the element's behavior if the TCP connection is closed by the RTSP server:
                - `:raise_error` - Raise an error.
                - `:send_eos` - Send an `:end_of_stream` to the output pad.
                """
              ],
              reconnect_attempts: [
                spec: non_neg_integer(),
                default: 0,
                description: """
                How many times to re-establish the RTSP session in place when it is lost
                during streaming (keep-alive failure, server closing the connection, a
                transport element crashing). Output pads and downstream links survive the
                reconnect; timestamps stay monotonic. `0` keeps the crashing behaviour.
                TCP transport only.
                """
              ],
              reconnect_delay: [
                spec: Time.t(),
                default: Time.seconds(1),
                default_inspector: &Time.pretty_duration/1,
                description: "Delay before each reconnect attempt."
              ]

  def_output_pad :output,
    accepted_format: _any,
    availability: :on_request

  defmodule State do
    @moduledoc false
    @type t :: %__MODULE__{
            stream_uri: binary(),
            allowed_media_types: ConnectionManager.media_types(),
            transport: Source.transport(),
            timeout: Time.t(),
            keep_alive_interval: Time.t(),
            tracks: [ConnectionManager.track()],
            rtsp_session: Membrane.RTSP.t() | nil,
            keep_alive_timer: reference() | nil,
            on_connection_closed: :raise_error | :send_eos,
            end_of_stream: boolean(),
            play_request_sent: boolean(),
            reconnect_attempts: non_neg_integer(),
            reconnect_delay: Time.t(),
            reconnects_left: non_neg_integer(),
            session_gen: non_neg_integer(),
            reconnect_timer: reference() | nil,
            linked_tracks: %{String.t() => Membrane.Pad.ref()}
          }

    @enforce_keys [
      :stream_uri,
      :allowed_media_types,
      :transport,
      :timeout,
      :keep_alive_interval,
      :on_connection_closed
    ]
    defstruct @enforce_keys ++
                [
                  reconnect_attempts: 0,
                  reconnect_delay: Time.seconds(1),
                  tracks: [],
                  ssrc_to_track: %{},
                  rtsp_session: nil,
                  keep_alive_timer: nil,
                  end_of_stream: false,
                  play_request_sent: false,
                  reconnects_left: 0,
                  session_gen: 0,
                  reconnect_timer: nil,
                  linked_tracks: %{}
                ]
  end

  @impl true
  def handle_init(_ctx, options) do
    state = struct(State, Map.from_struct(options))

    if state.reconnect_attempts > 0 and state.transport != :tcp do
      raise ArgumentError, "reconnect_attempts is supported for TCP transport only"
    end

    {[], %{state | reconnects_left: state.reconnect_attempts}}
  end

  @impl true
  def handle_setup(ctx, state) do
    state = ConnectionManager.establish_connection(ctx.utility_supervisor, state)

    {[spec: session_spec(state), notify_parent: get_set_up_tracks_notification(state)], state}
  end

  @impl true
  def handle_child_playing(_child, _ctx, %State{play_request_sent: false} = state) do
    {[], ConnectionManager.play(state)}
  end

  @impl true
  def handle_child_playing(_child, _ctx, state) do
    {[], state}
  end

  @impl true
  def handle_child_notification({:request_socket_control, _socket, pid}, :tcp_source, _ctx, state) do
    RTSP.transfer_socket_control(state.rtsp_session, pid)
    {[], state}
  end

  @impl true
  def handle_child_notification(notification, _element, _ctx, state) do
    Membrane.Logger.warning("Ignoring child notification: #{inspect(notification)}")
    {[], state}
  end

  @impl true
  def handle_info(
        {:DOWN, _ref, :process, rtsp_session, reason},
        ctx,
        %State{rtsp_session: rtsp_session} = state
      )
      when state.reconnects_left > 0 or state.reconnect_timer != nil do
    session_lost({:rtsp_session_down, reason}, ctx, %{state | rtsp_session: nil})
  end

  @impl true
  def handle_info(
        {:DOWN, _ref, :process, rtsp_session, reason},
        ctx,
        %State{rtsp_session: rtsp_session} = state
      ) do
    case state.on_connection_closed do
      :send_eos ->
        notify_udp_sources_actions =
          ctx.children
          |> Map.keys()
          |> Enum.filter(&match?({:udp_source, _ref}, &1))
          |> Enum.map(&{:notify_child, {&1, :close_socket}})

        {notify_udp_sources_actions, %{state | end_of_stream: true}}

      :raise_error ->
        {[terminate: {:rtsp_session_crash, reason}], state}
    end
  end

  @impl true
  def handle_info(:keep_alive, ctx, state) do
    if state.end_of_stream do
      {[], state}
    else
      case ConnectionManager.keep_alive(state) do
        {:ok, state} ->
          {[], state}

        {:error, reason} when state.reconnects_left > 0 ->
          session_lost({:keep_alive_failed, reason}, ctx, %{state | rtsp_session: nil})

        {:error, reason} ->
          {[terminate: {:shutdown, {:keep_alive_failed, reason}}], state}
      end
    end
  end

  @impl true
  def handle_info(:reconnect, ctx, state) do
    state = %{state | reconnect_timer: nil, play_request_sent: false}
    previous_tracks = state.tracks

    case ConnectionManager.try_establish_connection(ctx.utility_supervisor, state) do
      {:ok, state} ->
        if track_signatures(state.tracks) == track_signatures(previous_tracks) do
          Membrane.Logger.info("RTSP session re-established")
          state = %{state | session_gen: state.session_gen + 1}

          links = for {cp, _pad} <- state.linked_tracks, do: track_link_spec(cp, state)
          {[spec: [session_spec(state) | links]], state}
        else
          {[terminate: {:shutdown, :rtsp_tracks_changed}], state}
        end

      {:error, reason, state} ->
        session_lost({:reconnect_failed, reason}, ctx, state)
    end
  end

  @impl true
  def handle_info(message, _ctx, state) do
    Membrane.Logger.warning("Ignoring message: #{inspect(message)}")
    {[], state}
  end

  @impl true
  def handle_pad_added(Pad.ref(:output, control_path) = pad, _ctx, state) do
    track = Enum.find(state.tracks, &(&1.control_path == control_path))

    {demuxer_name, jitter_buffer_latency} =
      case state.transport do
        :tcp ->
          {:rtp_demuxer, 0}

        {:udp, _port_range_start, _port_range_end} ->
          {{:rtp_demuxer, track.control_path}, Time.milliseconds(200)}
      end

    if reconnect_enabled?(state) do
      chain =
        child({:funnel, control_path}, %Membrane.Funnel{end_of_stream: :never})
        |> child({:continuity, control_path}, Source.Continuity)
        |> depayloader(track)
        |> parser(track)
        |> bin_output(pad)

      state = put_in(state.linked_tracks[control_path], pad)
      {[spec: [chain, track_link_spec(control_path, state)]], state}
    else
      spec =
        get_child(demuxer_name)
        |> via_out(:output,
          options: [
            stream_id: {:payload_type, track.rtpmap.payload_type},
            jitter_buffer_latency: jitter_buffer_latency,
            clock_rate: track.rtpmap.clock_rate
          ]
        )
        |> depayloader(track)
        |> parser(track)
        |> bin_output(pad)

      {[spec: spec], state}
    end
  end

  # The session group died (transport crash, or we removed it after the RTSP
  # session was lost); schedule a reconnect or give up.
  @impl true
  def handle_crash_group_down({:rtsp_session, _gen}, ctx, state) do
    session_lost({:rtsp_session_crashed, ctx.crash_reason}, ctx, %{state | rtsp_session: nil})
  end

  # A funnel loses its input when the session group dies; that is expected.
  @impl true
  def handle_child_pad_removed(_child, _pad, _ctx, state), do: {[], state}

  @impl true
  def handle_terminate_request(_ctx, state) do
    ConnectionManager.teardown(state)
    {[terminate: :normal], state}
  end

  defp reconnect_enabled?(state), do: state.reconnect_attempts > 0

  defp session_spec(state) do
    if reconnect_enabled?(state),
      do:
        {create_sources_spec(state),
         group: {:rtsp_session, state.session_gen}, crash_group_mode: :temporary},
      else: create_sources_spec(state)
  end

  defp track_link_spec(control_path, state) do
    track = Enum.find(state.tracks, &(&1.control_path == control_path))

    get_child(:rtp_demuxer)
    |> via_out(:output,
      options: [
        stream_id: {:payload_type, track.rtpmap.payload_type},
        jitter_buffer_latency: 0,
        clock_rate: track.rtpmap.clock_rate
      ]
    )
    |> via_in(Pad.ref(:input, state.session_gen))
    |> get_child({:funnel, control_path})
  end

  defp session_lost(_reason, _ctx, %{reconnect_timer: timer} = state) when timer != nil,
    do: {[], state}

  defp session_lost(reason, ctx, %{reconnects_left: 0} = state) do
    {[terminate: {:shutdown, reason}], cancel_keep_alive(state) |> drop_session(ctx)}
  end

  defp session_lost(reason, ctx, state) do
    Membrane.Logger.warning(
      "RTSP session lost (#{inspect(reason)}), reconnecting in #{Time.pretty_duration(state.reconnect_delay)}"
    )

    timer =
      Process.send_after(self(), :reconnect, Time.as_milliseconds(state.reconnect_delay, :round))

    state = %{
      cancel_keep_alive(state)
      | reconnect_timer: timer,
        reconnects_left: state.reconnects_left - 1
    }

    remove =
      for name <- [:tcp_source, :tcp_decapsulator, :rtp_demuxer],
          Map.has_key?(ctx.children, name),
          do: name

    if state.rtsp_session, do: ConnectionManager.teardown(state)
    {if(remove == [], do: [], else: [remove_children: remove]), %{state | rtsp_session: nil}}
  end

  defp drop_session(state, _ctx), do: %{state | rtsp_session: nil}

  defp cancel_keep_alive(%{keep_alive_timer: nil} = state), do: state

  defp cancel_keep_alive(state) do
    Process.cancel_timer(state.keep_alive_timer)
    %{state | keep_alive_timer: nil}
  end

  defp track_signatures(tracks) do
    tracks
    |> Enum.map(
      &{&1.control_path, &1.type, &1.rtpmap && {&1.rtpmap.payload_type, &1.rtpmap.encoding}}
    )
    |> Enum.sort()
  end

  @spec get_set_up_tracks_notification(State.t()) :: set_up_tracks_notification()
  defp get_set_up_tracks_notification(state) do
    {:set_up_tracks, Enum.map(state.tracks, &Map.delete(&1, :transport))}
  end

  @spec create_sources_spec(State.t()) :: Membrane.ChildrenSpec.t()
  defp create_sources_spec(state) do
    payload_type_mapping =
      Map.new(
        state.tracks,
        fn %{rtpmap: rtpmap} ->
          {rtpmap.payload_type,
           %{encoding_name: String.to_atom(rtpmap.encoding), clock_rate: rtpmap.clock_rate}}
        end
      )

    case state.transport do
      :tcp ->
        {:tcp, socket} = List.first(state.tracks).transport

        child(:tcp_source, %Membrane.TCP.Source{
          connection_side: :client,
          local_socket: socket,
          on_connection_closed: state.on_connection_closed
        })
        |> child(:tcp_decapsulator, %RTSP.TCP.Decapsulator{rtsp_session: state.rtsp_session})
        |> child(:rtp_demuxer, %Membrane.RTP.Demuxer{payload_type_mapping: payload_type_mapping})

      {:udp, _port_range_start, _port_range_end} ->
        [
          Enum.flat_map(state.tracks, fn track ->
            {:udp, rtp_port, rtcp_port} = track.transport

            [
              child({:udp_source, rtp_port}, %Membrane.UDP.Source{local_port_no: rtp_port})
              |> child(
                {:rtp_demuxer, track.control_path},
                %Membrane.RTP.Demuxer{payload_type_mapping: payload_type_mapping}
              ),
              child({:udp_source, rtcp_port}, %Membrane.UDP.Source{local_port_no: rtcp_port})
              |> child(
                {:rtcp_demuxer, track.control_path},
                %Membrane.RTP.Demuxer{payload_type_mapping: payload_type_mapping}
              )
            ]
          end)
        ]
    end
  end

  @spec depayloader(ChildrenSpec.builder(), ConnectionManager.track()) :: ChildrenSpec.builder()
  defp depayloader(builder, track) do
    depayloader_definition =
      case {track.type, String.downcase(track.rtpmap.encoding)} do
        {_type, "h264"} ->
          Membrane.RTP.H264.Depayloader

        {_type, "h265"} ->
          Membrane.RTP.H265.Depayloader

        {_type, "opus"} ->
          Membrane.RTP.Opus.Depayloader

        {:audio, "mpeg4-generic"} ->
          mode =
            case track.fmtp do
              %{mode: :AAC_hbr} -> :hbr
              %{mode: :AAC_lbr} -> :lbr
            end

          %Membrane.RTP.AAC.Depayloader{mode: mode}

        {_type, _encoding} ->
          nil
      end

    if depayloader_definition != nil do
      child(builder, {:depayloader, make_ref()}, depayloader_definition)
    else
      builder
    end
  end

  @spec parser(ChildrenSpec.builder(), ConnectionManager.track()) :: ChildrenSpec.builder()
  defp parser(link_builder, %{rtpmap: %{encoding: "H264"}} = track) do
    sps = track.fmtp && track.fmtp.sprop_parameter_sets && track.fmtp.sprop_parameter_sets.sps
    pps = track.fmtp && track.fmtp.sprop_parameter_sets && track.fmtp.sprop_parameter_sets.pps

    child(link_builder, {:parser, make_ref()}, %Membrane.H264.Parser{
      spss: List.wrap(sps),
      ppss: List.wrap(pps),
      repeat_parameter_sets: true
    })
  end

  defp parser(link_builder, %{rtpmap: %{encoding: "H265"}} = track) do
    child(link_builder, {:parser, make_ref()}, %Membrane.H265.Parser{
      vpss: List.wrap(track.fmtp && track.fmtp.sprop_vps) |> Enum.map(&clean_parameter_set/1),
      spss: List.wrap(track.fmtp && track.fmtp.sprop_sps) |> Enum.map(&clean_parameter_set/1),
      ppss: List.wrap(track.fmtp && track.fmtp.sprop_pps) |> Enum.map(&clean_parameter_set/1),
      repeat_parameter_sets: true
    })
  end

  defp parser(link_builder, %{type: :audio, rtpmap: %{encoding: "mpeg4-generic"}} = track) do
    child(link_builder, {:parser, make_ref()}, %Membrane.AAC.Parser{
      audio_specific_config: track.fmtp.config
    })
  end

  defp parser(link_builder, _track), do: link_builder

  # a strange issue with one of Milesight camera where the parameter sets has
  # <<0, 0, 0, 1>> at the end
  @spec clean_parameter_set(binary()) :: binary()
  defp clean_parameter_set(ps) do
    case :binary.part(ps, byte_size(ps), -4) do
      <<0, 0, 0, 1>> -> :binary.part(ps, 0, byte_size(ps) - 4)
      _other -> ps
    end
  end
end
