package ro.tuc.dsrl.ds.handson.assig.one.client.views;

import java.awt.event.ActionListener;

import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.border.EmptyBorder;

import ro.tuc.dsrl.ds.handson.assig.one.client.entities.Student;

/**
 * @Author: Technical University of Cluj-Napoca, Romania Distributed Systems,
 *          http://dsrl.coned.utcluj.ro/
 * @Module: assignment-one-client
 * @Since: Sep 1, 2015
 * @Description: CatalogView is a JFrame which contains the UI elements of the
 *               Client application.
 */
public class CatalogView extends JFrame {

	private static final long serialVersionUID = 1L;
	private final JPanel contentPane;
	private final JTextField textFirstname;
	private final JTextField textLastname;
	private final JTextField textMail;
	private final JTextField textId;
	private final JButton btnGet;
	private final JButton btnPost;
	private final JButton btnDelete;
	private final JTextArea textArea;

	public CatalogView() {
		setTitle("HTTP Protocol simulator");
		setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		setBounds(300, 100, 450, 300);
		contentPane = new JPanel();
		contentPane.setBorder(new EmptyBorder(5, 5, 5, 5));
		setContentPane(contentPane);
		contentPane.setLayout(null);

		final JLabel lblInsertNewStudent = new JLabel("Insert new student");
		lblInsertNewStudent.setBounds(10, 11, 120, 14);
		contentPane.add(lblInsertNewStudent);

		final JLabel lblFirstname = new JLabel("First name");
		lblFirstname.setBounds(10, 36, 60, 14);
		contentPane.add(lblFirstname);

		final JLabel lblLastname = new JLabel("Last name");
		lblLastname.setBounds(10, 61, 60, 14);
		contentPane.add(lblLastname);

		final JLabel lblMail = new JLabel("Mail");
		lblMail.setBounds(10, 86, 46, 14);
		contentPane.add(lblMail);

		textFirstname = new JTextField();
		textFirstname.setBounds(80, 33, 86, 20);
		contentPane.add(textFirstname);
		textFirstname.setColumns(10);

		textLastname = new JTextField();
		textLastname.setBounds(80, 58, 86, 20);
		contentPane.add(textLastname);
		textLastname.setColumns(10);

		textMail = new JTextField();
		textMail.setBounds(80, 83, 86, 20);
		contentPane.add(textMail);
		textMail.setColumns(10);

		btnPost = new JButton("POST");
		btnPost.setBounds(10, 132, 89, 23);
		contentPane.add(btnPost);

		final JLabel lblFindStudentBy = new JLabel("Find student by id ");
		lblFindStudentBy.setBounds(235, 11, 145, 14);
		contentPane.add(lblFindStudentBy);

		final JLabel lblId = new JLabel("Id");
		lblId.setBounds(235, 36, 46, 14);
		contentPane.add(lblId);

		textId = new JTextField();
		textId.setBounds(320, 33, 86, 20);
		contentPane.add(textId);
		textId.setColumns(10);

		btnDelete = new JButton("DELETE");
		btnDelete.setBounds(340, 77, 89, 23);
		contentPane.add(btnDelete);

		btnGet = new JButton("GET");
		btnGet.setBounds(235, 77, 89, 23);
		contentPane.add(btnGet);

		textArea = new JTextArea();
		textArea.setBounds(235, 131, 171, 120);
		contentPane.add(textArea);
	}

	public void addBtnGetActionListener(ActionListener e) {
		btnGet.addActionListener(e);
	}

	public void addBtnPostActionListener(ActionListener e) {
		btnPost.addActionListener(e);
	}

	public void addBtnDeleteActionListener(ActionListener e) {
		btnDelete.addActionListener(e);
	}

	public String getStudentId() {
		return textId.getText();
	}

	public String getFirstname() {
		return textFirstname.getText();
	}

	public String getLastname() {
		return textLastname.getText();
	}

	public String getMail() {
		return textMail.getText();
	}

	public void printStudent(Student student) {
		textArea.setText(student.toString());
	}

	public void clear() {
		textId.setText("");
		textFirstname.setText("");
		textLastname.setText("");
		textMail.setText("");
	}
}
