import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.io.File;
import java.math.BigInteger;
import java.nio.file.Files;

public class ChordUI extends JFrame {
    private JTextField ipField = new JTextField("127.0.0.1", 12);
    private JTextField portField = new JTextField("5000", 6);
    private JTextField contactIpField = new JTextField(12);
    private JTextField contactPortField = new JTextField(6);
    private JTextArea logArea = new JTextArea(20, 50);
    private JTextField fileLookupField = new JTextField(20);
    private JButton clearBtn;

    private ChordNode node;
    BigInteger id;

    public ChordUI() {
        super("Chord Node UI");
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        buildUI();
        pack();
        setLocationRelativeTo(null);
    }

    private void buildUI() {
        JPanel top = new JPanel(new GridBagLayout());
        GridBagConstraints c = new GridBagConstraints();
        c.insets = new Insets(4, 6, 4, 6);
        c.anchor = GridBagConstraints.WEST;

        int row = 0;
        // Row 0: IP, Port, Start, Join, Leave
        c.gridy = row;
        c.gridx = 0;
        top.add(new JLabel("IP:"), c);
        c.gridx = 1;
        top.add(ipField, c);
        c.gridx = 2;
        top.add(new JLabel("Port:"), c);
        c.gridx = 3;
        top.add(portField, c);
        c.gridx = 4;
        JButton startBtn = new JButton("Start Node");
        startBtn.addActionListener(this::onStart);
        top.add(startBtn, c);
        c.gridx = 5;
        JButton joinBtn = new JButton("Join Contact");
        joinBtn.addActionListener(this::onJoin);
        top.add(joinBtn, c);
        c.gridx = 6;
        JButton leaveBtn = new JButton("Leave Node");
        leaveBtn.addActionListener(e -> runIfNode(n -> n.leave()));
        top.add(leaveBtn, c);

        // Row 1: Contact IP/Port, Stabilize, Fix, State
        row++;
        c.gridy = row;
        c.gridx = 0;
        top.add(new JLabel("Contact IP:"), c);
        c.gridx = 1;
        top.add(contactIpField, c);
        c.gridx = 2;
        top.add(new JLabel("Contact Port:"), c);
        c.gridx = 3;
        top.add(contactPortField, c);
        c.gridx = 4;
        JButton stabBtn = new JButton("Stabilize Now");
        stabBtn.addActionListener(e -> runIfNode(n -> n.stabilize()));
        top.add(stabBtn, c);
        c.gridx = 5;
        JButton fixBtn = new JButton("Fix Fingers Now");
        fixBtn.addActionListener(e -> runIfNode(n -> n.fixFingers()));
        top.add(fixBtn, c);
        c.gridx = 6;
        JButton stateBtn = new JButton("Show State");
        stateBtn.addActionListener(e -> updateStateDisplay());
        top.add(stateBtn, c);

        // Row 2: Upload, Lookup field and button
        row++;
        c.gridy = row;
        c.gridx = 0;
        JButton uploadBtn = new JButton("Upload File");
        uploadBtn.addActionListener(e -> onUpload());
        top.add(uploadBtn, c);
        c.gridx = 1;
        top.add(new JLabel("Lookup (name or id):"), c);
        c.gridx = 2;
        c.gridwidth = 2;
        c.fill = GridBagConstraints.HORIZONTAL;
        top.add(fileLookupField, c);
        c.gridwidth = 1;
        c.fill = GridBagConstraints.NONE;
        c.gridx = 4;
        JButton lookupBtn = new JButton("Lookup");
        lookupBtn.addActionListener(e -> onLookup());
        top.add(lookupBtn, c);
        c.gridx = 5;
        clearBtn = new JButton("Clear Log");
        clearBtn.addActionListener(e -> logArea.setText(""));
        top.add(clearBtn, c);

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
        // list local files
        if (node != null) {
            String[] files = node.fileStore.listFiles();
            append("Local files: " + String.join(",", files));
        }
    }

    private void append(String s) {
        SwingUtilities.invokeLater(() -> {
            String timestamp = String.format("[%tT] ", System.currentTimeMillis());
            logArea.append(timestamp + s + "\n");
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

    private void onUpload() {
        if (node == null) {
            append("Start a node first.");
            return;
        }
        JFileChooser chooser = new JFileChooser();
        int res = chooser.showOpenDialog(this);
        if (res != JFileChooser.APPROVE_OPTION)
            return;
        File f = chooser.getSelectedFile();
        try {
            byte[] data = Files.readAllBytes(f.toPath());
            BigInteger id = HashUtil.hash(f.getName());
            NodeInfo owner = node.findSuccessor(id);
            append("Uploading file '" + f.getName() + "' id=" + id + " -> owner=" + owner);
            boolean ok;
            if (owner.equals(node.self)) {
                node.fileStore.store(id, f.getName(), data);
                ok = true;
            } else {
                ok = RPC.storeFile(owner, id, f.getName(), data);
            }
            append(ok ? "Upload OK" : "Upload FAILED");
            updateStateDisplay();
        } catch (Exception ex) {
            append("Upload failed: " + ex.getMessage());
        }
    }

    private void onLookup() {

        if (node == null) {
            append("Start a node first.");
            return;
        }
        String q = fileLookupField.getText().trim();
        if (q.isEmpty()) {
            append("Enter filename or id to lookup.");
            return;
        }

        try {
            id = new BigInteger(q);
        } catch (Exception ex) {
            id = HashUtil.hash(q);
        }
        append("Looking up id=" + id);
        new Thread(() -> {
            NodeInfo owner = node.findSuccessor(id);
            append("Owner: " + owner);
            // offer download
            int choice = JOptionPane.showConfirmDialog(this, "Download from " + owner + "?", "Download?",
                    JOptionPane.YES_NO_OPTION);
            if (choice == JOptionPane.YES_OPTION) {
                try {
                    RPC.DownloadResult d = RPC.downloadFile(owner, id);
                    if (d == null) {
                        append("File not found at owner.");
                        return;
                    }
                    JFileChooser chooser = new JFileChooser();
                    chooser.setSelectedFile(new File(d.filename));
                    int r = chooser.showSaveDialog(this);
                    if (r != JFileChooser.APPROVE_OPTION)
                        return;
                    Files.write(chooser.getSelectedFile().toPath(), d.data);
                    append("Downloaded to " + chooser.getSelectedFile().getAbsolutePath());
                    updateStateDisplay();
                } catch (Exception ex) {
                    append("Download failed: " + ex.getMessage());
                }
            }
        }).start();
    }
}
