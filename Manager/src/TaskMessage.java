import java.io.Serializable;

/**
 * Created by arielbaruch on 07/12/14.
 */
public class TaskMessage implements Serializable{
	private static final long serialVersionUID = 1L;
	String uuid;
    int n;


    boolean terminate;

    public TaskMessage(String uuid, String inputFileName, String outputFileName, int n, boolean terminate) {
        this.uuid = uuid;
        this.n = n;
        this.terminate = terminate;
    }


    public String getUuid() {
        return uuid;
    }

    public int getN() {
        return n;
    }

    public boolean isTerminate() {
        return terminate;
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();

        sb.append(" uuid: " + uuid);
        sb.append(" n: " + n);
        sb.append(" terminate: " + terminate);
        sb.append(" ]");

        return sb.toString();
    }
}
