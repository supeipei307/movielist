package com.peisu.movie;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MovieMapper extends Mapper<LongWritable, Text, Text, Text> {

	private int lineCount = 0;
	private char delimiterChar = '\t';
	
	// Match Series Name, e.g. "#1 Single"
	private Pattern seriesNamePattern = Pattern.compile("^\".*\"");
	
	// Match Series Year, e.g. (2016/II)
	private Pattern seriesYearPattern = Pattern.compile("\\(\\d{4}.{0,4}\\)");
	
	// Match Movie Name, e.g. {Boyfriend Bathroom Bereavement (#1.6)}
	private Pattern movieNamePattern1 = Pattern.compile("\\{.+\\(.*\\)\\}");
	
	// Match Movie Name, e.g. #1 at the Apocalypse Box Office
	//                        $quid: The Movie
	//                        % (Percent)
	//                        & It's Politikal
	//                        '04: How Was It for You?
	//                        .hack//Osen kakudai vol. 1
	//                        Balloon Ascent at Crystal Palace
	private Pattern movieNamePattern2 = Pattern.compile("^[^\"].*\\(\\d");
	
	// Match Movie Year, e.g. 2016-????
	private Pattern movieYearPattern = Pattern.compile("[ \t]\\d{4}.{0,5}$");

	public void map(LongWritable ikey, Text ivalue, Context context) throws IOException, InterruptedException {

        String rawLine = ivalue.toString();
        String outputLine = new String();
        
        boolean hasMovieName = false;
        
        if (!rawLine.isEmpty()){
        	
        	Matcher seriesNameMatcher = seriesNamePattern.matcher(rawLine);
        	
        	Matcher seriesYearMatcher = seriesYearPattern.matcher(rawLine);
        	
        	Matcher movieNameMatcher1 = movieNamePattern1.matcher(rawLine);
        	Matcher movieNameMatcher2 = movieNamePattern2.matcher(rawLine);
        	
        	Matcher movieYearMatcher = movieYearPattern.matcher(rawLine);
        	
        	if (seriesNameMatcher.find()){
        		String seriesNameGroup = seriesNameMatcher.group();
        		outputLine = seriesNameGroup.substring(1, seriesNameGroup.length() - 1);
        	}
        	else
        		outputLine = "\\N";
        	
        	if (seriesYearMatcher.find()){
        		String seriesYearGroup = seriesYearMatcher.group();
        		int strRight = (seriesYearGroup.length() > 5)?5:seriesYearGroup.length() - 1;
        		outputLine = outputLine + delimiterChar + seriesYearGroup.substring(1, strRight);
        	}
        	else
        		outputLine = outputLine + delimiterChar + "\\N";
        	        	
        	if (movieNameMatcher1.find()){
        		String movieNameGroup = movieNameMatcher1.group();
        		outputLine = outputLine + delimiterChar + movieNameGroup.substring(1, movieNameGroup.length() - 1);
        		
        		hasMovieName = true;
        	}
        	else if (movieNameMatcher2.find()){
        		String movieNameGroup = movieNameMatcher2.group();
        		outputLine = outputLine + delimiterChar + movieNameGroup.substring(0, movieNameGroup.length() - 3).trim();
        		
        		hasMovieName = true;
        	}
        	else
        		outputLine = outputLine + delimiterChar + "\\N";
        	
        	if (movieYearMatcher.find()){
        		String movieYearGroup = movieYearMatcher.group();
        		int strRight = (movieYearGroup.length() > 5)?5:movieYearGroup.length();
        		outputLine = outputLine + delimiterChar + movieYearGroup.substring(1, strRight);
        	}
        	else
        		outputLine = outputLine + delimiterChar + "\\N";
        		//outputLine = outputLine + delimiterChar + "";
        	
        	if (hasMovieName){
        		lineCount++;
        	    context.write(new Text(Integer.toString(lineCount)), new Text(outputLine));
        	}
        }
	}
}
