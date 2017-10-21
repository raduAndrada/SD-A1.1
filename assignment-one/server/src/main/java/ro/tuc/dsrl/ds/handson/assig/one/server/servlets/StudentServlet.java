package ro.tuc.dsrl.ds.handson.assig.one.server.servlets;

import org.hibernate.cfg.Configuration;

import ro.tuc.dsrl.ds.handson.assig.one.protocol.encoders.ResponseMessageEncoder;
import ro.tuc.dsrl.ds.handson.assig.one.protocol.enums.StatusCode;
import ro.tuc.dsrl.ds.handson.assig.one.protocol.message.RequestMessage;
import ro.tuc.dsrl.ds.handson.assig.one.protocol.servlets.AbstractServlet;
import ro.tuc.dsrl.ds.handson.assig.one.server.dao.StudentDAO;
import ro.tuc.dsrl.ds.handson.assig.one.server.entities.Ct;

/**
 * @Author: Technical University of Cluj-Napoca, Romania Distributed Systems,
 *          http://dsrl.coned.utcluj.ro/
 * @Module: assignment-one-server
 * @Since: Sep 1, 2015
 * @Description: Serves for generating a response for the student related
 *               requests
 */
public class StudentServlet extends AbstractServlet {
	private final StudentDAO studentDao;

	public StudentServlet() {
		studentDao = new StudentDAO(new Configuration().configure().buildSessionFactory());
	}

	@Override
	public String doPost(RequestMessage message) {
		String response;

		// Attempt deserializing a Student object from the request
		final Ct student = message.getDeserializedObject(Ct.class);

		// Deserializing successful, add student to database and provide OK response
		if (student != null) {
			studentDao.addStudent(student);
			response = ResponseMessageEncoder.encode(StatusCode.OK, String.valueOf(student.getId()));
		} else {
			response = ResponseMessageEncoder.encode(StatusCode.BAD_REQUEST);
		}

		return response;
	}

	@Override
	public String doGet(RequestMessage message) {
		String response;

		// Get from the query values the desired id
		final String id = message.getQueryValues().get("id");
		// Find student in database and generate response
		if (id != null) {
			try {
				final Ct student = studentDao.findStudent(Integer.parseInt(id));

				if (student == null) {
					response = ResponseMessageEncoder.encode(StatusCode.NOT_FOUND);
				} else {
					response = ResponseMessageEncoder.encode(StatusCode.OK, student);
				}
			} catch (final NumberFormatException e) {
				response = ResponseMessageEncoder.encode(StatusCode.BAD_REQUEST);
			}
		}
		// Id missing from request
		else {
			response = ResponseMessageEncoder.encode(StatusCode.BAD_REQUEST);
		}

		return response;
	}

	@Override
	public String doDelete(RequestMessage message) {
		String response;

		// Attempt deserializing a Student object from the request
		final Ct student = message.getDeserializedObject(Ct.class);

		// Deserializing successful, add student to database and provide OK response
		if (student != null) {
			studentDao.deleteStudent(student.getId());
			response = ResponseMessageEncoder.encode(StatusCode.OK, String.valueOf(student.getId()));
		} else {
			response = ResponseMessageEncoder.encode(StatusCode.BAD_REQUEST);
		}

		return response;
	}
}
