#pragma once

// topic prefixes
inline constexpr const char* MMQP_BIRTH_STATUS_TOPIC_PREFIX = "$bwm/";
inline constexpr const char* MMQP_LAST_STATUS_TOPIC_PREFIX = "$lwm/";
inline constexpr const char* MMQP_SHARED_TOPIC_PREFIX = "$share/";

// topic delimiter    
inline constexpr char MMQP_TOPIC_WILDCARD_PLUS = '+';
inline constexpr char MMQP_TOPIC_WILDCARD_TAG = '#';
inline constexpr char MMQP_TOPIC_DELIMETER = '/';

// protocol configs
inline constexpr unsigned char MMQP_STRLEN_SIZE = 1;
inline constexpr unsigned char MMQP_FIXED_LEN = 1;

// protocol alias
inline constexpr const char* MMQP_ALIAS = "MMQP";
inline constexpr unsigned char MMQPv1 = 0x01;
using _byte_ = unsigned char;

// control packets
inline constexpr _byte_ MMQP_PACKET_CONREQ = 0x10;
inline constexpr _byte_ MMQP_PACKET_CONACK = 0x20;
inline constexpr _byte_ MMQP_PACKET_CONSYN = 0x30;
inline constexpr _byte_ MMQP_PACKET_CONEND = 0x40;
inline constexpr _byte_ MMQP_PACKET_PROREQ = 0x50;
inline constexpr _byte_ MMQP_PACKET_PROACK = 0x60;
inline constexpr _byte_ MMQP_PACKET_PROSYN = 0x70;
inline constexpr _byte_ MMQP_PACKET_PUBREQ = 0x80;
inline constexpr _byte_ MMQP_PACKET_PUBACK = 0x90;

// control masks
inline constexpr _byte_ MMQP_PACKET_TYPE_MASK = 0xF0;
inline constexpr _byte_ MMQP_RLEN_FLAGS_MASK = 0x0C;
inline constexpr _byte_ MMQP_CTRL_FLAGS_MASK = 0x03;
inline constexpr _byte_ MMQP_QOS_LEVEL_MASK = 0x03; 

// conreq flags
inline constexpr _byte_ MMQP_CONREQ_FLAGS_RES = 0x02;

// consyn flags
inline constexpr _byte_ MMQP_CONSYN_FLAGS_TOK = 0x02;

// session flags
inline constexpr _byte_ MMQP_SESSION_FLAGS_SAF = 0x80;
inline constexpr _byte_ MMQP_SESSION_FLAGS_SPF = 0x40;
inline constexpr _byte_ MMQP_SESSION_FLAGS_QPF = 0x20;
inline constexpr _byte_ MMQP_SESSION_FLAGS_SWF = 0x10;
inline constexpr _byte_ MMQP_SESSION_FLAGS_TPF = 0x08;
// compatibility names from the initial codebase
inline constexpr _byte_ MMQP_SESSION_FLAGS_SSF = 0x10;
inline constexpr _byte_ MMQP_SESSION_FLAGS_STF = 0x08;

// authen flags
inline constexpr _byte_ MMQP_AUTHEN_FLAGS_SID = 0x80;
inline constexpr _byte_ MMQP_AUTHEN_FLAGS_TOK = 0x40;

// policy flags
inline constexpr _byte_ MMQP_POLICY_FLAGS_IRS = 0x80;
inline constexpr _byte_ MMQP_POLICY_FLAGS_SEI = 0x40;
inline constexpr _byte_ MMQP_POLICY_FLAGS_KAI = 0x20;
inline constexpr _byte_ MMQP_POLICY_FLAGS_MPS = 0x10;

// wills flags
inline constexpr _byte_ MMQP_STATUS_FLAGS_BSM = 0x80;
inline constexpr _byte_ MMQP_STATUS_FLAGS_LSM = 0x40;
inline constexpr _byte_ MMQP_STATUS_FLAGS_TOP = 0x20;
inline constexpr _byte_ MMQP_STATUS_FLAGS_BWM = 0x80;
inline constexpr _byte_ MMQP_STATUS_FLAGS_LWM = 0x40;

// quic policy flags and allocation strategies (paper, page 13)
inline constexpr _byte_ MMQP_QUIC_FLAGS_SAS = 0x80;
inline constexpr _byte_ MMQP_QUIC_FLAGS_BTS = 0x40;
inline constexpr _byte_ MMQP_QUIC_FLAGS_QDG = 0x20;
inline constexpr _byte_ MMQP_QUIC_STRATEGY_CUSTOM = 0x00;
inline constexpr _byte_ MMQP_QUIC_STRATEGY_SINGLE = 0x01;
inline constexpr _byte_ MMQP_QUIC_STRATEGY_SEMANTIC = 0x04;
inline constexpr _byte_ MMQP_QUIC_STRATEGY_BULK = 0x03;

// profile flags
inline constexpr _byte_ MMQP_PROREQ_FLAGS_TOP = 0x20;
inline constexpr _byte_ MMQP_PROREQ_FLAGS_REQ = 0x10;
inline constexpr _byte_ MMQP_PROREQ_TYPE_MASK = 0xC0;
inline constexpr _byte_ MMQP_PROREQ_TYPE_UNS = 0xC0;
inline constexpr _byte_ MMQP_PROREQ_TYPE_REG = 0x80;
inline constexpr _byte_ MMQP_PROREQ_TYPE_SUB = 0x40;

// publish flags
inline constexpr _byte_ MMQP_PUBREQ_FLAGS_DUP = 0x80;
inline constexpr _byte_ MMQP_PUBREQ_FLAGS_E2E = 0x40;
inline constexpr _byte_ MMQP_PUBREQ_FLAGS_TOP = 0x20;
inline constexpr _byte_ MMQP_PUBREQ_TYPE_MASK = 0x03;
inline constexpr _byte_ MMQP_PUBREQ_TYPE_RES = 0x03;
inline constexpr _byte_ MMQP_PUBREQ_TYPE_REQ = 0x02;
inline constexpr _byte_ MMQP_PUBREQ_TYPE_MSG = 0x01;

// success codes
inline constexpr _byte_ MMQP_REASON_SUCCESS = 0x00;
inline constexpr _byte_ MMQP_REASON_NOSUBSCRIBER = 0x01;
inline constexpr _byte_ MMQP_REASON_ALIAS_MAPPED = 0x02;
inline constexpr _byte_ MMQP_REASON_UNSPECIFIED = 0x80;
inline constexpr _byte_ MMQP_REASON_AUTH_FAILED = 0x81;
inline constexpr _byte_ MMQP_REASON_BANNED = 0x82;
inline constexpr _byte_ MMQP_REASON_POLICY_VIOLATION = 0x83;
inline constexpr _byte_ MMQP_REASON_MALFORMED_PACKET = 0x84;
inline constexpr _byte_ MMQP_REASON_SESSION_EXPIRED = 0x85;
inline constexpr _byte_ MMQP_REASON_UNAUTHORIZED = 0x86;
inline constexpr _byte_ MMQP_REASON_KEEPALIVE_TIMEOUT = 0x87;
inline constexpr _byte_ MMQP_REASON_UNSUPPORTED_VERSION = 0x88;
inline constexpr _byte_ MMQP_REASON_INVALID_TOPIC = 0x89;
inline constexpr _byte_ MMQP_REASON_INVALID_ALIAS = 0x8A;
inline constexpr _byte_ MMQP_REASON_INVALID_PID = 0x8B;
inline constexpr _byte_ MMQP_REASON_INTERNAL_ERROR = 0x8C;
inline constexpr _byte_ MMQP_REASON_NO_RESPONDER = 0x8D;
inline constexpr _byte_ MMQP_REASON_BUSY = 0x8E;
inline constexpr _byte_ MMQP_REASON_TIMEOUT = 0x8F;
inline constexpr _byte_ MMQP_REASON_INVALID_CORRELATION = 0x90;

