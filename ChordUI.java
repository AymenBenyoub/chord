import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;

public class ChordUI extends JFrame {
    private JTextField ipField = new JTextField("127.0.0.1", 12);
    private JTextField portField = new JTextField("5000", 6);
    private JTextField contactIpField = new JTextField(12);
    private JTextField contactPortField = new JTextField(6);
    private JTextArea logArea = new JTextArea(20, 50);

    private ChordNode node;

    public ChordUI() {
        super("Chord Node UI");
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        buildUI();
        pack();
        setLocationRelativeTo(null);
    }

    private void buildUI() {
        JPanel top = new JPanel();
        top.add(new JLabel("IP:"));
        top.add(ipField);
        top.add(new JLabel("Port:"));
        top.add(portField);
        top.add(new JLabel("Contact IP:"));
        top.add(contactIpField);
        top.add(new JLabel("Contact Port:"));
        top.add(contactPortField);

        JButton startBtn = new JButton("Start Node");
        startBtn.addActionListener(this::onStart);
        top.add(startBtn);

        JButton joinBtn = new JButton("Join Contact");
        joinBtn.addActionListener(this::onJoin);
        top.add(joinBtn);

        JButton stabBtn = new JButton("Stabilize Now");
        stabBtn.addActionListener(e -> runIfNode(n -> n.stabilize()));
        top.add(stabBtn);

        JButton fixBtn = new JButton("Fix Fingers Now");
        fixBtn.addActionListener(e -> runIfNode(n -> n.fixFingers()));
        top.add(fixBtn);

        JButton stateBtn = new JButton("Show State");
        stateBtn.addActionListener(e -> updateStateDisplay());
        top.add(stateBtn);

        getContentPane().setLayout(new BorderLayout());
        getContentPane().add(top, BorderLayout.NORTH);

        logArea.setEditable(false);
        JScrollPane scroll = new JScrollPane(logArea);
        getContentPane().add(scroll, BorderLayout.CENTER);
    }

    private void onStart(ActionEvent e) {
        String ip = ipField.getText().trim();
        int port = Integer.parseInt(portField.getText().trim());
        node = new ChordNode(ip, port);
        new Server(node).start();
        append("Node started: " + node.self);

        // start periodic background maintenance
        new Thread(() -> {
            try {
                while (true) {
                    node.stabilize();
                    node.fixFingers();
                    SwingUtilities.invokeLater(this::updateStateDisplay);
                    Thread.sleep(15000);
                }
            } catch (InterruptedException ex) {
                // exit thread
            }
        }).start();
    }

    private void onJoin(ActionEvent e) {
        if (node == null) {
            append("Start a node first.");
            return;
        }
        String cip = contactIpField.getText().trim();
        String cport = contactPortField.getText().trim();
        if (cip.isEmpty() || cport.isEmpty()) {
            node.join(null);
            append("Created new ring (no contact provided).\n");
        } else {
            NodeInfo contact = new NodeInfo(null, cip, Integer.parseInt(cport));
            node.join(contact);
            append("Joined via contact: " + contact + "\n");
        }
        updateStateDisplay();
    }

    private void updateStateDisplay() {
        if (node == null)
            return;
        StringBuilder sb = new StringBuilder();
        sb.append("Self: ").append(node.self).append('\n');
        sb.append("Predecessor: ").append(node.predecessor).append('\n');
        sb.append("Successor: ").append(node.successor).append('\n');
        sb.append("Fingers:\n");
        for (int i = 0; i < node.finger.length; i++)
            sb.append("[" + i + "] -> ").append(node.finger[i]).append('\n');
        append(sb.toString());
    }

    private void append(String s) {
        SwingUtilities.invokeLater(() -> {
            logArea.append(s + "\n");
            logArea.setCaretPosition(logArea.getDocument().getLength());
        });
    }

    private void runIfNode(java.util.function.Consumer<ChordNode> c) {
        if (node == null) {
            append("Start a node first.");
            return;
        }
        new Thread(() -> {
            c.accept(node);
            SwingUtilities.invokeLater(this::updateStateDisplay);
        }).start();
    }
}
