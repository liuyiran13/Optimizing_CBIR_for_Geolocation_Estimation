package Test;

import java.awt.EventQueue;
import java.awt.RenderingHints;
import java.io.File;
import java.io.IOException;

import javax.imageio.ImageIO;
import javax.swing.ImageIcon;
import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.border.EmptyBorder;
import javax.swing.JLabel;

import MyAPI.General.General;

import java.awt.GridBagLayout;
import java.awt.GridBagConstraints;
import java.awt.Insets;

import javax.swing.JButton;

import java.awt.event.ActionListener;
import java.awt.event.ActionEvent;

@SuppressWarnings("serial")
public class GUI_test extends JFrame {

	private JPanel contentPane;

	/**
	 * Launch the application.
	 */
	public static void main(String[] args) {
		EventQueue.invokeLater(new Runnable() {
			public void run() {
				try {
					GUI_test frame = new GUI_test();
					frame.pack();
					frame.setVisible(true);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});
	}

	/**
	 * Create the frame.
	 * @throws IOException 
	 */
	@SuppressWarnings("unused")
	public GUI_test() throws IOException {
		String PhotoOriPath_3MFlickr="O:\\MediaEval_3185258Images\\trainImages_1-3185258\\";
		int PhotoIndex=1; int saveInterval=100*1000; int total_photos=3185258;  
		String folder=(PhotoIndex/saveInterval*saveInterval+1)+"-"+(PhotoIndex/saveInterval+1)*saveInterval;
		String filename=PhotoIndex+"_"+total_photos+".jpg";
		String photoPath=PhotoOriPath_3MFlickr+folder+"\\"+filename;
		String description="_ori";
		ImageIcon imageIcon_ori=createImageIcon_ori(photoPath,description);
		
		description="_small";
		ImageIcon imageIcon_small=createImageIcon_scaled( photoPath,  description, 128, 128, RenderingHints.VALUE_INTERPOLATION_BILINEAR, true);
		
		setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		setBounds(100, 100, 681, 519);
		contentPane = new JPanel();
		contentPane.setBorder(new EmptyBorder(5, 5, 5, 5));
		setContentPane(contentPane);
		GridBagLayout gbl_contentPane = new GridBagLayout();
		gbl_contentPane.columnWidths = new int[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
		gbl_contentPane.rowHeights = new int[]{0, 0, 0, 0, 0};
		gbl_contentPane.columnWeights = new double[]{0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, Double.MIN_VALUE};
		gbl_contentPane.rowWeights = new double[]{0.0, 0.0, 0.0, 0.0, Double.MIN_VALUE};
		contentPane.setLayout(gbl_contentPane);
		
		JLabel lblNewLabel_2 = new JLabel("New label");
		GridBagConstraints gbc_lblNewLabel_2 = new GridBagConstraints();
		gbc_lblNewLabel_2.insets = new Insets(0, 0, 5, 5);
		gbc_lblNewLabel_2.gridx = 2;
		gbc_lblNewLabel_2.gridy = 0;
		contentPane.add(lblNewLabel_2, gbc_lblNewLabel_2);
		
		JLabel lblNewLabel_3 = new JLabel("New label");
		GridBagConstraints gbc_lblNewLabel_3 = new GridBagConstraints();
		gbc_lblNewLabel_3.insets = new Insets(0, 0, 5, 5);
		gbc_lblNewLabel_3.gridx = 3;
		gbc_lblNewLabel_3.gridy = 0;
		contentPane.add(lblNewLabel_3, gbc_lblNewLabel_3);
		
		JButton btnNewButton = new JButton("New button");
		btnNewButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent arg0) {
			}
		});
		GridBagConstraints gbc_btnNewButton = new GridBagConstraints();
		gbc_btnNewButton.insets = new Insets(0, 0, 5, 0);
		gbc_btnNewButton.gridx = 19;
		gbc_btnNewButton.gridy = 1;
		contentPane.add(btnNewButton, gbc_btnNewButton);
		
		
	}
	
	/** Returns an ori ImageIcon, or null if the path was invalid. */
	protected ImageIcon createImageIcon_ori(String photoPath, String description) {
		if (new File(photoPath).exists()) {
	        return new ImageIcon(photoPath, description);
	    } else {
	        System.err.println("Couldn't find file: " + photoPath);
	        return null;
	    }
		
//		ImageIcon icon = createImageIcon("images/middle.gif");
//		. . .
//		label1 = new JLabel("Image and Text",
//		                    icon,
//		                    JLabel.CENTER);
//		//Set the position of the text, relative to the icon:
//		label1.setVerticalTextPosition(JLabel.BOTTOM);
//		label1.setHorizontalTextPosition(JLabel.CENTER);
//
//		label2 = new JLabel("Text-Only Label");
//		label3 = new JLabel(icon);
	}
	
	/** Returns an scaled ImageIcon, or null if the path was invalid. 
	 * @throws IOException */
	protected ImageIcon createImageIcon_scaled(String photoPath, String description, int targetWidth, int targetHeight, Object hint, boolean higherQuality) throws IOException {
		if (new File(photoPath).exists()) {
	        return new ImageIcon(General.getScaledInstance(ImageIO.read(new File(photoPath)) , targetWidth, targetHeight, hint, higherQuality), description);
	    } else {
	        System.err.println("Couldn't find file: " + photoPath);
	        return null;
	    }
	}

}
