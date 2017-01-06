package gui;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

/**
 * date: 13-6-9 上午10:27
 *
 * @author: yangyang.cong@ttpod.com
 */
public class Test {
    private JButton Hello;
    private JTextField textField1;

    public Test() {
        Hello.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                JOptionPane.showMessageDialog((Component) e.getSource(), "Eggs are not supposed to be green.");
            }
        });
    }
}
