package ta.logger;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * Created by arielbaruch on 07/12/14.
 */
public class LogMessage {
    private String ID;
    private String level;
    private String message;
    private DateTime time;


    public LogMessage(String ID, String level, String message) {
        this.time = new DateTime();
        this.ID = ID;
        this.message = message;
        this.level = level;
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        DateTimeFormatter fmt = DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss");
        String formattedTime = fmt.print(this.time);

        sb.append(formattedTime);
        sb.append(" "+ this.ID );
        sb.append(" ["+this.level+"] ");
        sb.append(message);
        return sb.toString();
    }
}
