package kafka.async;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.varia.LevelRangeFilter;
import org.junit.Before;

public class TestWithLog4j {

	@Before
	public void initConsole() {
		String PATTERN = "%d %p %c %m%n";
		LevelRangeFilter filter;

		ConsoleAppender stdout = new ConsoleAppender();
		stdout.setLayout(new PatternLayout(PATTERN));
		stdout.setTarget("System.out");
		stdout.setThreshold(Level.TRACE);
		
		filter =new LevelRangeFilter();
		filter.setLevelMin(Level.DEBUG);
		filter.setLevelMax(Level.INFO);
		stdout.addFilter(filter);
		
		stdout.activateOptions();
		
		
		ConsoleAppender stderr = new ConsoleAppender();
		stderr.setLayout(new PatternLayout(PATTERN));
		stderr.setTarget("System.err");
		stderr.setThreshold(Level.TRACE);

		filter = new LevelRangeFilter();
		filter.setLevelMin(Level.WARN);
		filter.setLevelMax(Level.FATAL);
		stderr.addFilter(filter);

		stderr.activateOptions();
		

		Logger.getRootLogger().removeAllAppenders();
		Logger.getRootLogger().setLevel(Level.TRACE);
		Logger.getRootLogger().addAppender(stdout);
		Logger.getRootLogger().addAppender(stderr);
		
	}
	
}
