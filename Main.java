import javax.swing.SwingUtilities;

public class Main {
    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            SwingUtilities.invokeLater(() -> {
                try {
                    new ChordUI().setVisible(true);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
            return;
        }

        String ip = args[0];
        int port = Integer.parseInt(args[1]);

        ChordNode node = new ChordNode(ip, port);
        new Server(node).start();

        if (args.length == 4) {
            NodeInfo contact = new NodeInfo(null, args[2], Integer.parseInt(args[3]));
            node.join(contact);
        }

        while (true) {
            node.stabilize();
            node.fixFingers();
            node.printState();
            Thread.sleep(15000);
        }
    }
}
