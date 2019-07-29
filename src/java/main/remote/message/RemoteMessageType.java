package remote.message;

public enum  RemoteMessageType {
    PUT,
    GET,
    DEL,
    STATUS,

    //response
    OK,
    GET_R,
    KEY_NOT_EXIST,
    SWITCH_NODE,
    STATUS_R,
    SERVICE_LOST,
    TIMEOUT,
    NOT_WRITE,
    NOT_READ,
    ERROR,
    UN_KNOWN,

}
