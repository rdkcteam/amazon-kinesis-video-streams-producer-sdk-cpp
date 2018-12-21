/*******************************************
Main internal include file
*******************************************/
#ifndef __MKV_GEN_INCLUDE_I__
#define __MKV_GEN_INCLUDE_I__

#pragma once

#ifdef  __cplusplus
extern "C" {
#endif

////////////////////////////////////////////////////
// Project include files
////////////////////////////////////////////////////
#include "com/amazonaws/kinesis/video/mkvgen/Include.h"

// For tight packing
#pragma pack(push, include_i, 1) // for byte alignment

////////////////////////////////////////////////////
// General defines and data structures
////////////////////////////////////////////////////

/**
 * MKV track types
 * Default track type for MKV = complex
 */
#define MKV_DEFAULT_TRACK_TYPE          0x03
#define MKV_TRACK_TYPE_VIDEO            0x01
#define MKV_TRACK_TYPE_AUDIO            0x02

/**
 * Content type prefixes
 */
#define MKV_CONTENT_TYPE_PREFIX_AUDIO       ((PCHAR) "audio/")
#define MKV_CONTENT_TYPE_PREFIX_VIDEO       ((PCHAR) "video/")

/**
 * Special processing for the types
 */
#define MKV_H264_CONTENT_TYPE               ((PCHAR) "video/h264")
#define MKV_H265_CONTENT_TYPE               ((PCHAR) "video/h265")
#define MKV_X_MKV_CONTENT_TYPE              ((PCHAR) "video/x-matroska")
#define MKV_FOURCC_CODEC_ID                 ((PCHAR) "V_MS/VFW/FOURCC")

/**
 * Generate a random byte
 */
#define MKV_GEN_RANDOM_BYTE()                     ((BYTE)(RAND() % 0x100))

/**
 * Static definitions
 */
extern BYTE gMkvHeaderBits[];
extern UINT32 gMkvHeaderBitsSize;
#define MKV_HEADER_BITS gMkvHeaderBits
#define MKV_HEADER_BITS_SIZE gMkvHeaderBitsSize

extern BYTE gMkvSegmentHeaderBits[];
extern UINT32 gMkvSegmentHeaderBitsSize;
#define MKV_SEGMENT_HEADER_BITS gMkvSegmentHeaderBits
#define MKV_SEGMENT_HEADER_BITS_SIZE gMkvSegmentHeaderBitsSize

extern BYTE gMkvSegmentInfoBits[];
extern UINT32 gMkvSegmentInfoBitsSize;
#define MKV_SEGMENT_INFO_BITS gMkvSegmentInfoBits
#define MKV_SEGMENT_INFO_BITS_SIZE gMkvSegmentInfoBitsSize

// Offset into gMkvSegmentInfoBits for fixing-up the SegmentUID
#define MKV_SEGMENT_UID_OFFSET 8

// Offset into gMkvSegmentInfoBits for fixing-up the timecode scale
#define MKV_SEGMENT_TIMECODE_SCALE_OFFSET 28

extern DOUBLE gMkvAACSamplingFrequencies[];
extern UINT32 gMkvAACSamplingFrequenciesCount;
#define MKV_AAC_SAMPLING_FREQUNECY_IDX_MAX gMkvAACSamplingFrequenciesCount
#define MKV_AAC_CHANNEL_CONFIG_MAX 8

extern BYTE gMkvTrackInfoBits[];
extern UINT32 gMkvTrackInfoBitsSize;
extern BYTE gMkvTracksElem[];
extern UINT32 gMkvTracksElemSize;
extern BYTE gMkvTrackVideoBits[];
extern UINT32 gMkvTrackVideoBitsSize;
extern BYTE gMkvCodecPrivateDataElem[];
extern UINT32 gMkvCodecPrivateDataElemSize;
extern BYTE gMkvTrackAudioBits[];
extern UINT32 gMkvTrackAudioBitsSize;
#define MKV_TRACK_INFO_BITS gMkvTrackInfoBits
#define MKV_TRACK_INFO_BITS_SIZE gMkvTrackInfoBitsSize
#define MKV_TRACKS_ELEM_BITS gMkvTracksElem
#define MKV_TRACKS_ELEM_BITS_SIZE gMkvTracksElemSize
#define MKV_TRACK_VIDEO_BITS gMkvTrackVideoBits
#define MKV_TRACK_VIDEO_BITS_SIZE gMkvTrackVideoBitsSize
#define MKV_CODEC_PRIVATE_DATA_ELEM gMkvCodecPrivateDataElem
#define MKV_CODEC_PRIVATE_DATA_ELEM_SIZE gMkvCodecPrivateDataElemSize
#define MKV_TRACK_AUDIO_BITS gMkvTrackAudioBits
#define MKV_TRACK_AUDIO_BITS_SIZE gMkvTrackAudioBitsSize

// The size of the track ID in bytes. We are using 8 bytes for the track ID
#define MKV_TRACK_ID_BYTE_SIZE 8

// gMkvTrackInfoBits element size offset for fixing up
#define MKV_TRACK_HEADER_SIZE_OFFSET 4

// gMkvTrackInfoBits track entry size offset for fixing up
#define MKV_TRACK_ENTRY_SIZE_OFFSET 1

// gMkvTrackInfoBits track ID offset for fixing up
#define MKV_TRACK_ID_OFFSET 11

// gMkvTrackInfoBits track type offset for fixing up
#define MKV_TRACK_TYPE_OFFSET 21

// gMkvTrackInfoBits track name offset for fixing up
#define MKV_TRACK_NAME_OFFSET 25

// gMkvTrackInfoBits Codec ID offset
#define MKV_CODEC_ID_OFFSET 59

// Track info video width offset for fixing up
#define MKV_TRACK_VIDEO_WIDTH_OFFSET 7

// Track info video height offset for fixing up
#define MKV_TRACK_VIDEO_HEIGHT_OFFSET 11

// Number of bytes to skip to get from Tracks to first TrackEntry
#define MKV_TRACK_ENTRY_OFFSET 8

// Number of bytes to skip to get from begin of TrackEntry to TrackNumber
#define MKV_TRACK_NUMBER_OFFSET 7

// Number of bytes to skip to get from begin of TrackEntry to TrackNumber
#define MKV_TRACK_TYPE_OFFSET 21

// Number of bytes to skip to get from begin of Audio to SamplingFrequency
#define MKV_TRACK_AUDIO_SAMPLING_RATE_OFFSET 7

// Number of bytes to skip to get from begin of TrackEntry to Channels
#define MKV_TRACK_AUDIO_CHANNELS_OFFSET 17

// Mkv TrackEntry element plus its data size
#define MKV_TRACK_ENTRY_HEADER_BITS_SIZE 5

// Minimum AAC codec private size
#define MIN_AAC_CPD_SIZE 2

extern BYTE gMkvClusterInfoBits[];
extern UINT32 gMkvClusterInfoBitsSize;
#define MKV_CLUSTER_INFO_BITS gMkvClusterInfoBits
#define MKV_CLUSTER_INFO_BITS_SIZE gMkvClusterInfoBitsSize

// gMkvClusterInfoBits cluster timecode offset for fixing up
#define MKV_CLUSTER_TIMECODE_OFFSET 7

extern BYTE gMkvSimpleBlockBits[];
extern UINT32 gMkvSimpleBlockBitsSize;
#define MKV_SIMPLE_BLOCK_BITS gMkvSimpleBlockBits
#define MKV_SIMPLE_BLOCK_BITS_SIZE gMkvSimpleBlockBitsSize

// gMkvSimpleBlockBits payload header size - Track number, timecode (2 bytes) and flags
#define MKV_SIMPLE_BLOCK_PAYLOAD_HEADER_SIZE 4

// gMkvSimpleBlockBits simple block element size offset for fixing up
#define MKV_SIMPLE_BLOCK_SIZE_OFFSET 1

// gMkvSimpleBlockBits block timecode offset for fixing up
#define MKV_SIMPLE_BLOCK_TIMECODE_OFFSET 10

// gMkvSimpleBlockBits simple block flags offset for fixing up
#define MKV_SIMPLE_BLOCK_FLAGS_OFFSET 12

// Offset to the track number field in mkv simple block
#define MKV_SIMPLE_BLOCK_TRACK_NUMBER_OFFSET 9

extern BYTE gMkvTagsBits[];
extern UINT32 gMkvTagsBitsSize;
#define MKV_TAGS_BITS gMkvTagsBits
#define MKV_TAGS_BITS_SIZE gMkvTagsBitsSize

extern BYTE gMkvTagNameBits[];
extern UINT32 gMkvTagNameBitsSize;
#define MKV_TAG_NAME_BITS gMkvTagNameBits
#define MKV_TAG_NAME_BITS_SIZE gMkvTagNameBitsSize

extern BYTE gMkvTagStringBits[];
extern UINT32 gMkvTagStringBitsSize;
#define MKV_TAG_STRING_BITS gMkvTagStringBits
#define MKV_TAG_STRING_BITS_SIZE gMkvTagStringBitsSize

// gMkvTagsBits tags element size offset for fixing up
#define MKV_TAGS_ELEMENT_SIZE_OFFSET 4

// gMkvTagsBits tag element offset for fixing up
#define MKV_TAG_ELEMENT_OFFSET 12

// gMkvTagsBits tag element size offset for fixing up
#define MKV_TAG_ELEMENT_SIZE_OFFSET 14

// gMkvTagsBits simple tag element offset for fixing up
#define MKV_SIMPLE_TAG_ELEMENT_OFFSET 22

// gMkvTagsBits simple tag element size offset for fixing up
#define MKV_SIMPLE_TAG_ELEMENT_SIZE_OFFSET 24

// gMkvTagNameBits simple tag name element size offset for fixing up
#define MKV_TAG_NAME_ELEMENT_SIZE_OFFSET 2

// gMkvTagStringBits simple tag string element size offset for fixing up
#define MKV_TAG_STRING_ELEMENT_SIZE_OFFSET 2


/**
 * MKV simple block flags
 */
#define MKV_SIMPLE_BLOCK_FLAGS_NONE                 0x00
#define MKV_SIMPLE_BLOCK_KEY_FRAME_FLAG             0x80
#define MKV_SIMPLE_BLOCK_INVISIBLE_FLAG             0x08
#define MKV_SIMPLE_BLOCK_DISCARDABLE_FLAG           0x01

/**
 * MKV block overhead in bytes
 */
#define MKV_SIMPLE_BLOCK_OVERHEAD    (MKV_SIMPLE_BLOCK_BITS_SIZE)

/**
 * MKV cluster overhead in bytes
 */
#define MKV_CLUSTER_OVERHEAD     (MKV_CLUSTER_INFO_BITS_SIZE + MKV_SIMPLE_BLOCK_OVERHEAD)

/**
 * MKV EBML and Segment header size in bytes
 */
#define MKV_EBML_SEGMENT_SIZE               (MKV_HEADER_BITS_SIZE + MKV_SEGMENT_HEADER_BITS_SIZE)

/**
 * MKV Segment and Track info
 */
#define GET_MKV_SEGMENT_TRACK_INFO_SIZE(track_count)        (MKV_SEGMENT_INFO_BITS_SIZE + MKV_TRACK_INFO_BITS_SIZE * track_count + MKV_TRACKS_ELEM_BITS_SIZE)

/**
 * MKV header size in bytes
 */
#define GET_MKV_HEADER_SIZE(track_count)        (MKV_EBML_SEGMENT_SIZE + GET_MKV_SEGMENT_TRACK_INFO_SIZE(track_count))

/**
 * MKV header overhead in bytes
 */
#define GET_MKV_HEADER_OVERHEAD(track_count)        (GET_MKV_HEADER_SIZE(track_count) + MKV_CLUSTER_OVERHEAD)

/**
 * MKV Segment and Track info overhead in bytes
 */
#define GET_MKV_SEGMENT_TRACK_INFO_OVERHEAD(track_count)        (GET_MKV_SEGMENT_TRACK_INFO_SIZE(track_count) + MKV_CLUSTER_OVERHEAD)

/**
 * To and from MKV timestamp conversion factoring in the timecode
 */
#define TIMESTAMP_TO_MKV_TIMECODE(ts, tcs)   ((ts) * DEFAULT_TIME_UNIT_IN_NANOS / (tcs))
#define MKV_TIMECODE_TO_TIMESTAMP(tc, tcs)   ((tc) * ((tcs) / DEFAULT_TIME_UNIT_IN_NANOS))

/**
 * The rest of the internal include files
 */
#include "NalAdapter.h"
#include "SpsParser.h"

/**
 * MKV stream states enum
 */
typedef enum {
    MKV_GENERATOR_STATE_START,
    MKV_GENERATOR_STATE_SEGMENT_HEADER,
    MKV_GENERATOR_STATE_CLUSTER_INFO,
    MKV_GENERATOR_STATE_SIMPLE_BLOCK,
    MKV_GENERATOR_STATE_TAGS,
} MKV_GENERATOR_STATE, *PMKV_GENERATOR_STATE;

/**
 * MkvGenerator internal structure
 */
typedef struct {
    // The original public structure
    MkvGenerator mkvGenerator;

    // Whether to do clustering decision based on key frames only
    BOOL keyFrameClustering;

    // Whether to use in-stream timestamps
    BOOL streamTimestamps;

    // Whether to use absolute timestamps in clusters
    BOOL absoluteTimeClusters;

    // Timecode scale
    UINT64 timecodeScale;

    // Clusters desired duration
    UINT64 clusterDuration;

    // The content type of the stream
    BYTE trackType;

    // Time function entry
    GetCurrentTimeFunc getTimeFn;

    // Custom data to be passed to the callback
    UINT64 customData;

    // Generator internal state
    MKV_GENERATOR_STATE generatorState;

    // The latest cluster start timestamp
    UINT64 lastClusterPts;

    // The latest cluster start timestamp
    UINT64 lastClusterDts;

    // The timestamp of the beginning of the stream
    UINT64 streamStartTimestamp;

    // Whether we have stored the timestamp for clusters in case of relative cluster timestamps
    BOOL streamStartTimestampStored;

    // NALUs adaptation
    MKV_NALS_ADAPTATION nalsAdaptation;

    // Whether to adapt CPD NALs from Annex-B to Avcc format.
    BOOL adaptCpdNals;

    // Video height and width - Only for video
    UINT16 videoWidth;
    UINT16 videoHeight;

    // Array of TrackInfo containing track metadata
    PTrackInfo trackInfoList;

    // Number of TrackInfo object in trackInfoList
    UINT32 trackInfoCount;

} StreamMkvGenerator, *PStreamMkvGenerator;

////////////////////////////////////////////////////
// Internal functionality
////////////////////////////////////////////////////
/**
 * Gets the size overhead packaging to MKV
 *
 * @PStreamMkvGenerator - the current generator object
 * @FRAME_FLAGS - frame flags to use in calculation
 * @UINT64 - frame presentation timestamp
 *
 * @return - current state of the stream
 **/
MKV_STREAM_STATE mkvgenGetStreamState(PStreamMkvGenerator, FRAME_FLAGS, UINT64);

/**
 * Gets the size overhead packaging to MKV
 *
 * @PStreamMkvGenerator - the current generator object
 * @MKV_STREAM_STATE - State of the MKV parser
 *
 * @return - Size of the overhead in bytes
 **/
UINT32 mkvgenGetFrameOverhead(PStreamMkvGenerator, MKV_STREAM_STATE);

/**
 * Gets the size of the MKV header
 * @PStreamMkvGenerator - the current generator object
 *
 * @return - Size of the header in bytes
 */
UINT32 mkvgenGetHeaderOverhead(PStreamMkvGenerator);

/**
 * Fast validation of the frame object
 *
 * @PStreamMkvGenerator - the current generator object
 * @PFrame - Frame object
 * @PUINT64 - OUT - Extracted presentation timestamp
 * @PUINT64 - OUT - Extracted decoding timestamp
 * @PUINT64 - OUT - Extracted frame duration
 * @PMKV_STREAM_STATE - OUT - Current stream state
 *
 * @return - STATUS code of the execution
 **/
STATUS mkvgenValidateFrame(PStreamMkvGenerator, PFrame, PUINT64, PUINT64, PUINT64, PMKV_STREAM_STATE);

/**
 * Returns the MKV track type from the provided content type
 *
 * @PCHAR - content type to convert
 *
 * @return - MKV track type corresponding to the content type
 */
BYTE mkvgenGetTrackTypeFromContentType(PCHAR);

/**
 * EBML encodes a number and stores in the buffer
 *
 * @UINT64 - the number to encode
 * @PBYTE - the buffer to store the number in
 * @UINT32 - the size of the buffer
 * @PUINT32 - the returned encoded length in bytes
 */
STATUS mkvgenEbmlEncodeNumber(UINT64, PBYTE, UINT32, PUINT32);

/**
 * Stores a number in the buffer in a big-endian way
 *
 * @UINT64 - the number to encode
 * @PBYTE - the buffer to store the number in
 * @UINT32 - the size of the buffer
 * @PUINT32 - the returned encoded length in bytes
 */
STATUS mkvgenBigEndianNumber(UINT64, PBYTE, UINT32, PUINT32);

/**
 * EBML encodes a header and stores in the buffer
 *
 * @PBYTE - the buffer to store the encoded info in
 * @UINT32 - the size of the buffer
 * @PUINT32 - the returned encoded length of the number in bytes
 */
STATUS mkvgenEbmlEncodeHeader(PBYTE, UINT32, PUINT32);

/**
 * EBML encodes a segment header and stores in the buffer
 *
 * @PBYTE - the buffer to store the encoded info in
 * @UINT32 - the size of the buffer
 * @PUINT32 - the returned encoded length of the number in bytes
 */
STATUS mkvgenEbmlEncodeSegmentHeader(PBYTE, UINT32, PUINT32);

/**
 * EBML encodes a segment info and stores in the buffer
 *
 * @PBYTE - the buffer to store the encoded info in
 * @UINT32 - the size of the buffer
 * @UINT64 - default timecode scale
 * @PUINT32 - the returned encoded length of the number in bytes
 */
STATUS mkvgenEbmlEncodeSegmentInfo(PBYTE, UINT32, UINT64, PUINT32);

/**
 * EBML encodes a track info and stores in the buffer
 *
 * @PBYTE - the buffer to store the encoded info in
 * @UINT32 - the size of the buffer
 * @PStreamMkvGenerator - The MKV generator
 * @PUINT32 - the returned encoded length of the number in bytes
 */
STATUS mkvgenEbmlEncodeTrackInfo(PBYTE, UINT32, PStreamMkvGenerator, PUINT32);

/**
 * EBML encodes a cluster header and stores in the buffer
 *
 * @PBYTE - the buffer to store the encoded info in
 * @UINT32 - the size of the buffer
 * @UINT64 - cluster timestamp
 * @PUINT32 - the returned encoded length of the number in bytes
 */
STATUS mkvgenEbmlEncodeClusterInfo(PBYTE, UINT32, UINT64, PUINT32);

/**
 * EBML encodes a simple block and stores in the buffer
 *
 * @PBYTE - the buffer to store the encoded info in
 * @UINT32 - the size of the buffer
 * @INT16 - frame timestamp
 * @PFrame - the frame to encode
 * @UINT32 - the adapted frame size
 * @PStreamMkvGenerator - The MKV generator
 * @PUINT32 - the returned encoded length of the number in bytes
 */
STATUS mkvgenEbmlEncodeSimpleBlock(PBYTE, UINT32, INT16, PFrame, UINT32, PStreamMkvGenerator, PUINT32);

/**
 * Returns the byte count of a number
 *
 * @UINT64 - the number to process
 *
 * @return - the number of bytes required.
 */
UINT32 mkvgenGetByteCount(UINT64);

/**
 * Adapts the getTime function
 *
 * @UINT64 - the custom data passed in
 *
 * @return - the current time.
 */
UINT64 getTimeAdapter(UINT64);

/**
 * Parse out sampling frequency and channel config from aac cpd
 *
 * @PBYTE - The buffer storing aac codec private data
 * @UINT32 - Size of the codec private data
 * @PDOUBLE - The returned sampling frequency
 * @PUINT16 - The returned channel config
 */
STATUS getSamplingFreqAndChannelFromAacCpd(PBYTE, UINT32, PDOUBLE, PUINT16);

#pragma pack(pop, include_i)

#ifdef  __cplusplus
}
#endif
#endif  /* __MKV_GEN_INCLUDE_I__ */
