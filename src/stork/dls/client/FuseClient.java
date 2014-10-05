package stork.dls.client;

import stork.dls.stream.DLSStream;
import stork.dls.stream.DLSStream.CHANNEL_STATE;
import stork.dls.util.DLSLog;

/**
 * the base class of FTP client each transferring through this client retains
 * the channel state
 * 
 * @see DLSFTPMetaChannel
 */
public class FuseClient{
    public static boolean DEBUG_PRINT;
    public static boolean DEBUG_PIPELINE = false;
    private static DLSLog logger = DLSLog.getLog(FuseClient.class.getName());
    public DLSStream.CHANNEL_STATE channelstate = CHANNEL_STATE.CC_BENIGN;
    protected String clientID = null;
    protected int port;
    protected String host;
    public final static int ONE_PIPE_CAPACITY = 1;//DLSFTPMetaChannel.ONE_PIPE_CAPACITY;
    protected final DLSStream stream;

    public FuseClient(String host, int port, DLSStream input) {
        this.stream = input;
        this.host = host;
        this.port = port;
    }

    /**
     * for the pipe capacity constructor
     * 
     * @param localmetachannel
     * @param input
     */
    public FuseClient(DLSStream input) {
        this.stream = input;
    }

    public DLSStream getStream() {
        return stream;
    }

    public void printHP() {
        System.out.println("host = " + this.host + "; port = " + this.port);
    }

    public void setHP(String host, int port) {
        this.host = host;
        this.port = port;
    }

    // close data channel
    public void closeDC() {
    	
    }
}
