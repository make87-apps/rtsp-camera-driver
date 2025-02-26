import logging

from make87_messages.core.header_pb2 import Header
import make87
import av
from datetime import datetime, timedelta

from make87_messages.video.any_pb2 import FrameAny
from make87_messages.video.frame_av1_pb2 import FrameAV1
from make87_messages.video.frame_h264_pb2 import FrameH264
from make87_messages.video.frame_h265_pb2 import FrameH265


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Generic function for encoding frames
def encode_frame(codec, header, data, width, height, is_keyframe) -> FrameAny:
    codec_classes = {
        "h264": FrameH264,
        "hevc": FrameH265,
        "av1": FrameAV1,
    }

    if codec not in codec_classes:
        raise ValueError(f"Unsupported codec: {codec}")

    sub_message = codec_classes[codec](header=header, data=data, width=width, height=height, is_keyframe=is_keyframe)

    return FrameAny(header=header, **{codec: sub_message})


def main():
    make87.initialize()
    topic = make87.get_publisher(name="VIDEO_DATA", message_type=FrameAny)

    # Read Camera Configuration
    config = {
        "username": make87.get_config_value("CAMERA_USERNAME"),
        "password": make87.get_config_value("CAMERA_PASSWORD"),
        "ip": make87.get_config_value("CAMERA_IP"),
        "port": make87.get_config_value("CAMERA_PORT", default=554, decode=int),
        "suffix": make87.get_config_value("CAMERA_URI_SUFFIX", default=""),
        "stream_index": make87.get_config_value("STREAM_INDEX", default=0, decode=int),
    }

    stream_uri = f"rtsp://{config['username']}:{config['password']}@{config['ip']}:{config['port']}/{config['suffix']}"
    with av.open(stream_uri) as container:
        stream_start = datetime.now()  # Reference timestamp

        # Find the requested video stream
        video_stream = next(iter(s for s in container.streams if s.index == config["stream_index"]), None)
        if video_stream is None:
            raise ValueError(f"Stream index {config['stream_index']} not found.")

        # Print stream information
        stream_info = {
            "Index": video_stream.index,
            "Codec": video_stream.codec_context.name,
            "Resolution": f"{video_stream.width}x{video_stream.height}",
            "Pixel Format": video_stream.pix_fmt,
            "Frame Rate": str(video_stream.average_rate),
        }
        logger.info("Stream Attributes:", stream_info)

        # Validate codec support
        codec_name = video_stream.codec_context.name
        if codec_name not in {"h264", "hevc", "av1"}:
            raise ValueError(f"Unsupported codec: {codec_name}")

        # Stream metadata
        start_pts = video_stream.start_time or 0  # Handle missing start_time
        time_base = float(video_stream.time_base)
        width, height = video_stream.width, video_stream.height

        keyframe_found = False

        for packet in container.demux(video_stream):
            if packet.dts is None or (not keyframe_found and not packet.is_keyframe):
                continue  # Skip until the first keyframe appears

            keyframe_found = True

            # Compute timestamps
            packet_pts = packet.pts or packet.dts
            relative_timestamp = (packet_pts - start_pts) * time_base
            absolute_timestamp = stream_start + timedelta(seconds=relative_timestamp)

            header = Header(entity_path=f"/camera/{config['ip']}/{config['suffix']}")
            header.timestamp.FromDatetime(absolute_timestamp)

            # Encode and publish the frame
            topic.publish(encode_frame(codec_name, header, bytes(packet), width, height, packet.is_keyframe))


if __name__ == "__main__":
    main()
