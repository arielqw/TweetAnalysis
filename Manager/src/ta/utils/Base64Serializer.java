package ta.utils;

import com.amazonaws.util.Base64;

import java.io.*;

/**
 * Created by arielbaruch on 07/12/14.
 */
public class Base64Serializer {

    public static Object deSerialize( String s ){
        Object res = null;
        try {
            res = _deSerialize(s);
        } catch (ClassNotFoundException e) {
            System.out.println("Error: deSerialize failed.");
            e.printStackTrace();
        } catch (IOException e) {
            System.out.println("Error: deSerialize failed.");
            e.printStackTrace();
        }

        return res;
    }
    public static String serialize( Serializable o ) {
        String res = null;
        try {
            res = _serialize(o);
        } catch (IOException e) {
            System.out.println("Error: serialize failed.");
            e.printStackTrace();
        }

        return res;
    }

        /** Read the object from Base64 string. */
    private static Object _deSerialize( String s ) throws IOException,
            ClassNotFoundException {
        byte [] data = Base64.decode( s );
        ObjectInputStream ois = new ObjectInputStream(
                new ByteArrayInputStream(  data ) );
        Object o  = ois.readObject();
        ois.close();
        return o;
    }

    /** Write the object to a Base64 string. */
    private static String _serialize( Serializable o ) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream( baos );
        oos.writeObject( o );
        oos.close();
        return new String( Base64.encode(baos.toByteArray()) );
    }

}
