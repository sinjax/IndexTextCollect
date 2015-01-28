package com.evi.knowledge.terrier;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;

import org.apache.log4j.xml.DOMConfigurator;
import org.terrier.utility.ApplicationSetup;

/**
 * Utility to help enable the use of Terrier without having 
 * to install anything. Taken from ImageTerrier
 * 
 * @author Sina Samangooei <ss@ecs.soton.ac.uk> 
 *
 */
public class BasicTerrierConfig {
	/**
	 * log4j configuration
	 */
	public static final String DEFAULT_LOG4J_CONFIG = 
		  "<?xml version=\"1.0\" encoding=\"UTF-8\" ?>"
		+ "<!DOCTYPE log4j:configuration SYSTEM \"log4j.dtd\">" 
		+ "<log4j:configuration xmlns:log4j=\"http://jakarta.apache.org/log4j/\">"
		+ " <appender name=\"console\" class=\"org.apache.log4j.ConsoleAppender\">"
		+ "  <param name=\"Target\" value=\"System.err\"/>"
		+ "  <layout class=\"org.apache.log4j.SimpleLayout\"/>"
		+ " </appender>"
		+ " <logger name=\"org.terrier\">"
		+ "  <level value=\"__LEVEL__\" />"
		+ "  <appender-ref ref=\"console\" />"
		+ " </logger>"
		+ " <logger name=\"org.imageterrier\">"
		+ "  <level value=\"__LEVEL__\" />"
		+ "  <appender-ref ref=\"console\" />"
		+ " </logger>"
		+ "</log4j:configuration>";
	
	/**
	 * The log level property string
	 */
	public static String LOG_LEVEL = "imageterrier.log.level";
	
	private static boolean isConfigured = false;
	
	/**
	 * This method attempts to aid the circumvention of the terrier ApplicationSetup badness!
	 * We should really re-write that bit of terrier...
	 */
	public static synchronized void configure() {
		if (isConfigured)
			return;
		
		try {
			File tmp = File.createTempFile("terrier-tmp-", ".properties");
			
			System.setProperty("terrier.home", tmp.getParent());
			System.setProperty("terrier.setup", tmp.getAbsolutePath());

			ApplicationSetup.setProperty("termpipelines", "");
			ApplicationSetup.setProperty("querying.postprocesses.order", "");
			ApplicationSetup.loadCommonProperties(); //THIS IS REQUIRED TO PROPERLY READ PREFS (ApplicationSetup is a POS!!)
			
			ApplicationSetup.TERRIER_INDEX_PREFIX = "index"; //imageterrier convention
			
			//reconfigure log4j
			new DOMConfigurator().doConfigure(new StringReader(DEFAULT_LOG4J_CONFIG.replace("__LEVEL__", System.getProperty(LOG_LEVEL, "warn"))), org.apache.log4j.LogManager.getLoggerRepository());
			
			tmp.delete();
			isConfigured = true;
			
		} catch (IOException e) {
			e.printStackTrace(System.err);
			System.err.println("Error with terrier configuration");
			System.exit(1);
		}
	}
}
