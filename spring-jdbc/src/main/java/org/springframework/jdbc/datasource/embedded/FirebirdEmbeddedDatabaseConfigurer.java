package org.springframework.jdbc.datasource.embedded;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import javax.sql.DataSource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.firebirdsql.gds.impl.jni.EmbeddedGDSFactoryPlugin;
import org.firebirdsql.jdbc.FBDriver;
import org.firebirdsql.management.FBManager;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;

public class FirebirdEmbeddedDatabaseConfigurer implements EmbeddedDatabaseConfigurer {

	private final Log logger = LogFactory.getLog(getClass());
	
	private static final String DB_PATH = "target/embedded-example.fdb";
	private static final String DB_USER = "sysdba";
	private static final String URL_TEMPLATE = "jdbc:firebirdsql:embedded:%s?charSet=utf-8";
	
	private static FirebirdEmbeddedDatabaseConfigurer instance;
	
	private FBManager fbManager;

	/**
	 * Get the singleton {@link FirebirdEmbeddedDatabaseConfigurer} instance.
	 * @return the configurer
	 */
	public static synchronized FirebirdEmbeddedDatabaseConfigurer getInstance() {
		if (instance == null) {
			instance = new FirebirdEmbeddedDatabaseConfigurer();
		}
		return instance;
	}

	private FirebirdEmbeddedDatabaseConfigurer() {
		this.fbManager = new FBManager(EmbeddedGDSFactoryPlugin.EMBEDDED_TYPE_NAME);
	}
	
	@Override
	public void configureConnectionProperties(ConnectionProperties properties, String databaseName) {
		properties.setDriverClass(FBDriver.class);
		String format = String.format(URL_TEMPLATE, DB_PATH);
		properties.setUrl(format);
		properties.setUsername(DB_USER);
		properties.setPassword("");
		
		configureJna();
		open();
	}

	private void open() {
		logger.info("Embedded firebird - starting");
		try {
			fbManager.start();
		} catch (Exception e) {
			throw new BeanCreationException("Error starting embedded Firebird", e);
		}
		try {
			Path parent = Paths.get(DB_PATH).getParent();
			if (!Files.exists(parent)) {
				Files.createDirectories(parent);
			} else if (!Files.isDirectory(parent)) {
				throw new BeanCreationException("Cannot create " + DB_PATH + " file. " + parent + " is not a directory.");
			}
			fbManager.createDatabase(DB_PATH, DB_USER, "");
		} catch (Exception e) {
			throw new BeanCreationException("Error creating embedded Firebird", e);
		}
		logger.info("Embedded firebird - started");
	}

	@Override
	public void shutdown(DataSource dataSource, String databaseName) {
		logger.info("Embedded firebird - closing");
		try {
			fbManager.dropDatabase(DB_PATH, DB_USER, "");
		} catch (Exception e) {
			throw new BeanCreationException("Error deleting embedded Firebird", e);
		}
		try {
			fbManager.stop();
		} catch (Exception e) {
			throw new BeanCreationException("Error stopping embedded Firebird", e);
		}
		logger.info("Embedded firebird - closed");
	}

	private void configureJna() {
		Resource r = new ClassPathResource("firebird");
		Path path;
		try {
			path = Paths.get(r.getURI());
		} catch (IOException e) {
			throw new BeanCreationException("Error configuring JNA", e);
		}
		logger.info("Attempting to set jna.library.path to: " + path);
		System.setProperty("jna.library.path", path.toString());
	}
}
