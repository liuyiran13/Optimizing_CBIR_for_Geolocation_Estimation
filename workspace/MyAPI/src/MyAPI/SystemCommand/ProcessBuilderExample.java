package MyAPI.SystemCommand;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

public class ProcessBuilderExample
{
  
  public static void main(String[] args) throws Exception
  {
    new ProcessBuilderExample();
  }

  // can run basic ls or ps commands
  // can run command pipelines
  // can run sudo command if you know the password is correct
  public ProcessBuilderExample() throws IOException, InterruptedException
  {
	  
	//**********solution1***************
    // build the system command we want to run
    List<String> commands = Arrays.asList("cmd", "/c", "dir");
//    commands.add("/bin/sh");
//    commands.add("-c");
//    commands.add("ls -l /var/tmp | grep tmp");
    
    // execute the command
    SystemCommandExecutor commandExecutor = new SystemCommandExecutor(commands);
    int result = commandExecutor.executeCommand();

    // get the stdout and stderr from the command that was run
    StringBuilder stdout = commandExecutor.getStandardOutputFromCommand();
    StringBuilder stderr = commandExecutor.getStandardErrorFromCommand();
    
    // print the stdout and stderr
    System.out.println("The numeric result of the command was: " + result);
    System.out.println("STDOUT:");
    System.out.println(stdout);
    System.out.println("STDERR:");
    System.out.println(stderr);
    
	//**********solution2***************
	String s = null;
	try {        
		// run the Unix "ps -ef" command
	    // using the Runtime exec method:
	    Process p = Runtime.getRuntime().exec("cmd /c dir");
	    
	    BufferedReader stdInput = new BufferedReader(new 
	         InputStreamReader(p.getInputStream()));
	
	    BufferedReader stdError = new BufferedReader(new 
	         InputStreamReader(p.getErrorStream()));
	
	    // read the output from the command
	    System.out.println("Here is the standard output of the command:\n");
	    while ((s = stdInput.readLine()) != null) {
	        System.out.println(s);
	    }
	    
	    // read any errors from the attempted command
	    System.out.println("Here is the standard error of the command (if any):\n");
	    while ((s = stdError.readLine()) != null) {
	        System.out.println(s);
	    }
	    
	    System.exit(0);
	}
	catch (IOException e) {
	    System.out.println("exception happened - here's what I know: ");
	    e.printStackTrace();
	    System.exit(-1);
	}
    
    
  }
}
