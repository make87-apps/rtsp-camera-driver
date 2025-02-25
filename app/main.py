from make87_messages.core.header_pb2 import Header
import make87

from make87_messages.video.any_pb2 import FrameAny
import av
from datetime import datetime, timedelta
import logging

from make87_messages.video.frame_av1_pb2 import FrameAV1
from make87_messages.video.frame_h264_pb2 import FrameH264
from make87_messages.video.frame_h265_pb2 import FrameH265

FFMPEG_CODEC_MAPPING = {
    "h264": "h264",
    "h265": "hevc",
    "av1": "libaom-av1",
}


def encode_h264_frame(header: Header, data: bytes, width: int, height: int, is_keyframe: bool) -> FrameAny:
    sub_message = FrameH264(header=header, data=data, width=width, height=height, is_keyframe=is_keyframe)
    message = FrameAny(
        header=header,
        h264=sub_message,
    )
    return message


def encode_h265_frame(header: Header, data: bytes, width: int, height: int, is_keyframe: bool) -> FrameAny:
    sub_message = FrameH265(header=header, data=data, width=width, height=height, is_keyframe=is_keyframe)
    message = FrameAny(
        header=header,
        h265=sub_message,
    )
    return message


def encode_av1_frame(header: Header, data: bytes, width: int, height: int, is_keyframe: bool) -> FrameAny:
    sub_message = FrameAV1(header=header, data=data, width=width, height=height, is_keyframe=is_keyframe)
    message = FrameAny(
        header=header,
        av1=sub_message,
    )
    return message


ENCODING_FUNC_MAPPING = {
    "h264": encode_h264_frame,
    "h265": encode_h265_frame,
    "av1": encode_av1_frame,
}


def main():
    make87.initialize()
    topic = make87.get_publisher(name="VIDEO_DATA", message_type=FrameAny)

    username = make87.get_config_value("CAMERA_USERNAME")
    password = make87.get_config_value("CAMERA_PASSWORD")
    ip = make87.get_config_value(
        "CAMERA_IP",
    )
    port = make87.get_config_value("CAMERA_PORT", default=554, decode=int)
    suffix = make87.get_config_value("CAMERA_URI_SUFFIX", default="")

    codec = make87.get_config_value("VIDEO_CODEC", default="h264")

    stream_index = make87.get_config_value("STREAM_INDEX", default=0, decode=int)

    if codec not in FFMPEG_CODEC_MAPPING:
        raise ValueError(
            f"Unsupported video codec: {codec}. Supported codecs are: {', '.join(FFMPEG_CODEC_MAPPING.keys())}"
        )

    stream_uri = f"rtsp://{username}:{password}@{ip}:{port}/{suffix}"

    container = av.open(stream_uri)
    stream_start = datetime.now()  # Unix timestamp in seconds

    try:
        video_stream = next((s for s in container.streams if s.index == stream_index))
    except StopIteration:
        raise Exception(f"Configured stream index {stream_index} does not exist. Exiting.")

    # Basic stream information
    stream_info = {
        "Index": video_stream.index,
        "Type": video_stream.type,
        "Codec": video_stream.codec_context.name,
        "Profile": video_stream.profile,
        "Time Base": str(video_stream.time_base),
        "Start Time": video_stream.start_time,
        "Duration": video_stream.duration,
        "Frames": video_stream.frames,
        "Width": video_stream.codec_context.width,
        "Height": video_stream.codec_context.height,
        "Pixel Format": video_stream.codec_context.pix_fmt,
        "Average Frame Rate": str(video_stream.average_rate),
    }

    logging.info("Stream Attributes:")
    for key, value in stream_info.items():
        logging.info(f"  {key}: {value}")

    codec_name = video_stream.codec_context.name
    if codec_name != FFMPEG_CODEC_MAPPING[codec]:
        raise ValueError(f"Configured codec {codec} does not match stream codec {codec_name}. Exiting.")

    frame_buffer = bytearray()
    current_packet_time = None
    last_absolute_timestamp: datetime | None = None
    last_width: int = 0
    last_height: int = 0
    last_is_keyframe: bool = False

    for packet in container.demux(video_stream):
        if packet.dts is None:
            continue

        # Use pts if available; otherwise fall back to dts
        packet_time = packet.pts if packet.pts is not None else packet.dts
        frame_pts = packet_time * video_stream.time_base
        absolute_timestamp = stream_start + timedelta(seconds=float(frame_pts))

        # If we haven't started a frame yet, initialize our marker
        if current_packet_time is None:
            current_packet_time = packet_time

        # If the packet timestamp changes, then the previous frame is complete.
        if packet_time is not None and packet_time != current_packet_time:
            header = Header(entity_path=f"/camera/{ip}/{suffix}")
            header.timestamp.FromDatetime(last_absolute_timestamp)
            encode_func = ENCODING_FUNC_MAPPING[codec]
            message = encode_func(
                header=header,
                data=bytes(frame_buffer),
                width=last_width,
                height=last_height,
                is_keyframe=last_is_keyframe,
            )
            topic.publish(message)
            frame_buffer = bytearray()
            current_packet_time = packet_time

        # Store values for the next frame before processing
        last_absolute_timestamp = absolute_timestamp
        last_width = packet.stream.width
        last_height = packet.stream.height
        last_is_keyframe = packet.is_keyframe

        # Extend the current frame buffer with the packet's raw bytes
        frame_buffer.extend(bytes(packet))


if __name__ == "__main__":
    main()
