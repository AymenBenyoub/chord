import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.util.*;

public class ChordUI extends JFrame {
    private JTextField ipField = new JTextField("127.0.0.1", 12);
    private JTextField portField = new JTextField("5000", 6);
    private JTextField filePortField = new JTextField("6000", 6);
    private JTextField contactIpField = new JTextField(12);
    private JTextField contactPortField = new JTextField(6);
    private JTextArea logArea = new JTextArea(20, 50);
    private DefaultListModel<NodeInfo> peersModel = new DefaultListModel<>();
    private JList<NodeInfo> peersList = new JList<>(peersModel);
    private JButton sendFileBtn = new JButton("Send File (placeholder)");

    private ChordNode node;
    private static ChordUI instance;

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
        top.add(new JLabel("File Port:"));
        top.add(filePortField);

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

        // Left sidebar: peers list + send button
        JPanel left = new JPanel(new BorderLayout());
        peersList.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
        peersList.setVisibleRowCount(10);
        peersList.addListSelectionListener(e -> {
            NodeInfo sel = peersList.getSelectedValue();
            sendFileBtn.setEnabled(sel != null);
        });
        JScrollPane peersScroll = new JScrollPane(peersList);
        left.add(new JLabel("Peers"), BorderLayout.NORTH);
        left.add(peersScroll, BorderLayout.CENTER);
        sendFileBtn.setEnabled(false);
        sendFileBtn.addActionListener(e -> {
            NodeInfo sel = peersList.getSelectedValue();
            if (sel == null)
                return;
            JFileChooser chooser = new JFileChooser();
            int ret = chooser.showOpenDialog(this);
            if (ret != JFileChooser.APPROVE_OPTION)
                return;
            java.io.File file = chooser.getSelectedFile();
            append("Sending " + file.getName() + " to " + sel + "...");

            // progress dialog
            JDialog dlg = new JDialog(this, "Sending: " + file.getName(), true);
            JProgressBar bar = new JProgressBar(0, 100);
            bar.setStringPainted(true);
            dlg.getContentPane().add(bar);
            dlg.setSize(400, 80);
            dlg.setLocationRelativeTo(this);

            new Thread(() -> {
                boolean[] result = new boolean[1];
                result[0] = node.sendFile(sel, file, (sent, total) -> {
                    int pct = total == 0 ? 0 : (int) ((sent * 100) / total);
                    SwingUtilities.invokeLater(() -> bar.setValue(pct));
                });
                SwingUtilities.invokeLater(() -> {
                    dlg.dispose();
                    append((result[0] ? "File sent: " : "File send failed: ") + file.getName());
                });
            }).start();

            dlg.setVisible(true);
        });
        left.add(sendFileBtn, BorderLayout.SOUTH);

        getContentPane().add(left, BorderLayout.WEST);

        logArea.setEditable(false);
        JScrollPane scroll = new JScrollPane(logArea);
        getContentPane().add(scroll, BorderLayout.CENTER);
    }

    private void onStart(ActionEvent e) {
        String ip = ipField.getText().trim();
        int port = Integer.parseInt(portField.getText().trim());
        int filePort;
        try {
            filePort = Integer.parseInt(filePortField.getText().trim());
        } catch (Exception ex) {
            filePort = port + 1000;
        }
        node = new ChordNode(ip, port, filePort);
        instance = this;
        new Server(node).start();
        append("Node started: " + node.self);

        // start periodic background maintenance
        new Thread(() -> {
            try {
                while (true) {
                    node.stabilize();
                    node.fixFingers();
                    SwingUtilities.invokeLater(() -> {
                        updateStateDisplay();
                        refreshPeers();
                    });
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
        refreshPeers();
    }

    private void refreshPeers() {
        if (node == null)
            return;
        Set<NodeInfo> set = new LinkedHashSet<>();
        if (node.successor != null)
            set.add(node.successor);
        if (node.predecessor != null)
            set.add(node.predecessor);
        if (node.finger != null) {
            for (NodeInfo f : node.finger)
                if (f != null)
                    set.add(f);
        }

        // remove self from peers list
        set.remove(node.self);

        // update model
        SwingUtilities.invokeLater(() -> {
            peersModel.clear();
            for (NodeInfo p : set)
                peersModel.addElement(p);
        });
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

    public static void notifyFileReceived(java.io.File f) {
        if (instance != null)
            instance.showFileReceived(f);
    }

    private void showFileReceived(java.io.File f) {
        SwingUtilities.invokeLater(() -> {
            int r = JOptionPane.showOptionDialog(this,
                    "Received file: " + f.getName() + "\nSaved: " + f.getAbsolutePath(),
                    "File Received",
                    JOptionPane.DEFAULT_OPTION,
                    JOptionPane.INFORMATION_MESSAGE,
                    null,
                    new String[] { "Open Folder", "OK" },
                    "OK");
            if (r == 0) {
                try {
                    java.awt.Desktop.getDesktop().open(f.getParentFile());
                } catch (Exception ex) {
                    append("Could not open folder: " + ex.getMessage());
                }
            }
        });
    }
}
