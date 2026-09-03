defmodule Membrane.RTSP.Source.Continuity do
  @moduledoc false
  # Keeps timestamps monotonic across RTSP session reconnects. A new session's
  # demuxer restarts its clock, and the funnel upstream announces the new input with a
  # `Membrane.Funnel.NewInputEvent`, after which the next
  # buffer is offset to land one frame after the last one forwarded, and a
  # discontinuity event tells downstream parsers to drop partial state.

  use Membrane.Filter

  def_input_pad :input, accepted_format: _any
  def_output_pad :output, accepted_format: _any

  @gap Membrane.Time.milliseconds(33)

  @impl true
  def handle_init(_ctx, _opts), do: {[], %{offset: 0, last_pts: nil, resync?: false}}

  @impl true
  def handle_event(:input, %Membrane.Funnel.NewInputEvent{}, _ctx, state),
    do: {[], %{state | resync?: true}}

  def handle_event(pad, event, ctx, state), do: super(pad, event, ctx, state)

  @impl true
  def handle_buffer(:input, %{pts: nil} = buffer, _ctx, state),
    do: {[buffer: {:output, buffer}], state}

  def handle_buffer(:input, buffer, _ctx, %{resync?: true, last_pts: last} = state)
      when last != nil do
    state = %{state | offset: last + @gap - buffer.pts, resync?: false}
    {actions, state} = forward(buffer, state)
    {[event: {:output, %Membrane.Event.Discontinuity{}}] ++ actions, state}
  end

  def handle_buffer(:input, buffer, _ctx, state), do: forward(buffer, %{state | resync?: false})

  defp forward(buffer, state) do
    buffer = %{
      buffer
      | pts: buffer.pts + state.offset,
        dts: buffer.dts && buffer.dts + state.offset
    }

    {[buffer: {:output, buffer}], %{state | last_pts: buffer.pts}}
  end
end
