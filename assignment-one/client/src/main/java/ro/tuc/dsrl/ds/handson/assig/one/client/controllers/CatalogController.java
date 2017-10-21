package ro.tuc.dsrl.ds.handson.assig.one.client.controllers;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.IOException;

import javax.swing.JOptionPane;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import ro.tuc.dsrl.ds.handson.assig.one.client.communication.ServerConnection;
import ro.tuc.dsrl.ds.handson.assig.one.client.entities.Student;
import ro.tuc.dsrl.ds.handson.assig.one.client.views.CatalogView;
import ro.tuc.dsrl.ds.handson.assig.one.protocol.encoders.RequestMessageEncoder;
import ro.tuc.dsrl.ds.handson.assig.one.protocol.encoders.ResponseMessageEncoder;
import ro.tuc.dsrl.ds.handson.assig.one.protocol.enums.ProtocolMethod;
import ro.tuc.dsrl.ds.handson.assig.one.protocol.enums.StatusCode;
import ro.tuc.dsrl.ds.handson.assig.one.protocol.message.ResponseMessage;

/**
 * @Author: Technical University of Cluj-Napoca, Romania Distributed Systems,
 *          http://dsrl.coned.utcluj.ro/
 * @Module: assignment-one-client
 * @Since: Sep 1, 2015
 * @Description: Controller for the interface elements of the client.
 */
public class CatalogController {
	private static final Log LOGGER = LogFactory.getLog(CatalogController.class);

	private static final String ERROR_MESSAGE = "An exception occured while trying to send data to server. Please consult logs for more info!";
	private static final String HOST = "localhost";
	private static final int PORT = 8888;

	private final CatalogView catalogView;
	private final ServerConnection serverConnection;

	public CatalogController() {
		catalogView = new CatalogView();
		catalogView.setVisible(true);

		serverConnection = new ServerConnection(HOST, PORT);

		catalogView.addBtnGetActionListener(new GetActionListener());
		catalogView.addBtnPostActionListener(new PostActionListener());
		catalogView.addBtnDeleteActionListener(new DeleteActionListener());
	}

	public void printStudent(Student student) {
		if (student != null) {
			catalogView.printStudent(student);
		}
	}

	public void displayErrorMessage(String message) {
		catalogView.clear();
		JOptionPane.showMessageDialog(catalogView, message, "Error", JOptionPane.ERROR_MESSAGE);
	}

	public void displayInfoMessage(String message) {
		catalogView.clear();
		JOptionPane.showMessageDialog(catalogView, message, "Success", JOptionPane.PLAIN_MESSAGE);
	}

	/**
	 * Provides functionality for the POST button.
	 */
	class PostActionListener implements ActionListener {
		@Override
		public void actionPerformed(ActionEvent e) {
			final String firstname = catalogView.getFirstname();
			final String lastname = catalogView.getLastname();
			final String mail = catalogView.getMail();

			if (!("".equals(firstname) || "".equals(lastname) || "".equals(mail))) {
				final Student student = new Student();
				student.setFirstname(firstname);
				student.setLastname(lastname);
				student.setMail(mail);

				try {
					// encode the request: a POST request, at the url "student", sending the student
					// object
					final String encodedRequest = RequestMessageEncoder.encode(ProtocolMethod.POST, "student", student);
					final String response = serverConnection.sendRequest(encodedRequest);
					// decode the response from server
					final ResponseMessage decodedResponse = ResponseMessageEncoder.decode(response);

					// if server responded OK, then operation was successful, else display error
					if (decodedResponse.getStatusCode() == StatusCode.OK.getCode()) {
						displayInfoMessage("Successfully inserted; id=" + decodedResponse.getSerializedObject());
					} else {
						displayErrorMessage("Status code " + decodedResponse.getStatusCode());
					}

				} catch (final IOException ex) {
					LOGGER.info(ex.getMessage());
					displayErrorMessage(ex.getMessage());
				}
			} else {
				displayErrorMessage("Please fill all textboxes before submiting!");
			}
		}
	}

	/**
	 * Provides functionality for the GET button.
	 */
	class GetActionListener implements ActionListener {

		@Override
		public void actionPerformed(ActionEvent e) {
			try {
				final int studentId = Integer.parseInt(catalogView.getStudentId());
				// encode the request: a GET request, with url "student?id=X" (passing id in
				// url) and no object sent through
				final String encodedRequest = RequestMessageEncoder.encode(ProtocolMethod.GET,
						"student?id=" + studentId);
				final String response = serverConnection.sendRequest(encodedRequest);
				final ResponseMessage decodedResponse = ResponseMessageEncoder.decode(response);

				if (decodedResponse.getStatusCode() == StatusCode.OK.getCode()) {
					printStudent(decodedResponse.getDeserializedObject(Student.class));
				} else {
					displayErrorMessage("Status code " + decodedResponse.getStatusCode());
				}
			} catch (final NumberFormatException ex) {
				displayErrorMessage("Please enter a number!");
			} catch (final IOException ex) {
				displayErrorMessage(ERROR_MESSAGE);
				LOGGER.error("", ex);
			}
		}
	}

	/**
	 * Provide functionality for the delete button
	 */

	class DeleteActionListener implements ActionListener {
		@Override
		public void actionPerformed(ActionEvent e) {
			try {
				final Student student = new Student();
				student.setId(Integer.parseInt(catalogView.getStudentId()));
				// encode the request: a POST request, at the url "delete", sending the student
				// object
				final String encodedRequest = RequestMessageEncoder.encode(ProtocolMethod.DELETE, "student", student);
				final String response = serverConnection.sendRequest(encodedRequest);
				// decode the response from server
				final ResponseMessage decodedResponse = ResponseMessageEncoder.decode(response);

				// if server responded OK, then operation was successful, else display error
				if (decodedResponse.getStatusCode() == StatusCode.OK.getCode()) {
					displayInfoMessage("Successfully deleted; id=" + decodedResponse.getSerializedObject());
				} else {
					displayErrorMessage("Status code " + decodedResponse.getStatusCode());
				}

			} catch (final NumberFormatException ex) {
				displayErrorMessage("Please enter a number!");
			} catch (final IOException ex) {
				displayErrorMessage(ERROR_MESSAGE);
				LOGGER.error("", ex);
			}
		}

	}
}
