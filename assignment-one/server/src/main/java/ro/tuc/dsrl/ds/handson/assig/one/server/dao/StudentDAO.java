package ro.tuc.dsrl.ds.handson.assig.one.server.dao;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hibernate.HibernateException;
import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;

import ro.tuc.dsrl.ds.handson.assig.one.server.entities.Ct;

/**
 * @Author: Technical University of Cluj-Napoca, Romania Distributed Systems,
 *          http://dsrl.coned.utcluj.ro/
 * @Module: assignment-one-server
 * @Since: Sep 1, 2015
 * @Description: Uses Hibernate for CRUD operations on the underlying database.
 *               The Hibernate configuration files can be found in the
 *               src/main/resources folder
 */
public class StudentDAO {
	private static final Log LOGGER = LogFactory.getLog(StudentDAO.class);

	private final SessionFactory factory;

	public StudentDAO(SessionFactory factory) {
		this.factory = factory;
	}

	public Ct addStudent(Ct student) {
		int studentId = -1;
		final Session session = factory.openSession();
		Transaction tx = null;
		try {
			tx = session.beginTransaction();
			studentId = (Integer) session.save(student);
			student.setId(studentId);
			tx.commit();
		} catch (final HibernateException e) {
			if (tx != null) {
				tx.rollback();
			}
			LOGGER.error("", e);
		} finally {
			session.close();
		}
		return student;
	}

	@SuppressWarnings("unchecked")
	public List<Ct> findStudents() {
		final Session session = factory.openSession();
		Transaction tx = null;
		List<Ct> students = null;
		try {
			tx = session.beginTransaction();
			students = session.createQuery("FROM Student").list();
			tx.commit();
		} catch (final HibernateException e) {
			if (tx != null) {
				tx.rollback();
			}
			LOGGER.error("", e);
		} finally {
			session.close();
		}
		return students;
	}

	@SuppressWarnings("unchecked")
	public Ct findStudent(int id) {
		final Session session = factory.openSession();
		Transaction tx = null;
		List<Ct> students = null;
		try {
			tx = session.beginTransaction();
			final Query query = session.createQuery("FROM Student WHERE id = :id");
			query.setParameter("id", id);
			students = query.list();
			tx.commit();
		} catch (final HibernateException e) {
			if (tx != null) {
				tx.rollback();
			}
			LOGGER.error("", e);
		} finally {
			session.close();
		}
		return students != null && !students.isEmpty() ? students.get(0) : null;
	}

	public Ct deleteStudent(int id) {
		final Ct student = findStudent(id);
		final Session session = factory.openSession();
		Transaction tx = null;
		try {
			tx = session.beginTransaction();
			session.delete(student);
			tx.commit();

		} catch (final HibernateException e) {
			if (tx != null) {
				tx.rollback();
			}
			LOGGER.error("", e);
			return student;
		} finally {
			session.close();
		}
		return student;
	}
}
