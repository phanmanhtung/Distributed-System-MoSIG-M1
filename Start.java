import java.lang.Integer;
public class Start {
    public static void main(String[] argv) throws Exception {

        if (argv.length < 1) {
            return;
        }

        OverlayNode node = new OverlayNode();
        node.start(Integer.parseInt(argv[0]));
    }
}
