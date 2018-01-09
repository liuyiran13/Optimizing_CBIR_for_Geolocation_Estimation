package MyAPI.SystemCommand;

import java.io.*;
import java.util.Arrays;
import java.util.List;

import MyAPI.General.General;

/**
 * my modifid version from SystemCommandExecutor, as this one do not use multithead to read stdout, 
 * in hadoop, it may not allow to use multithead in one hadoop-node!
 * 
 * This class can be used to execute a system command from a Java application.
 * See the documentation for the public methods of this class for more
 * information.
 * 
 * Documentation for this class is available at this URL:
 * 
 * http://devdaily.com/java/java-processbuilder-process-system-exec
 *
 * 
 * Copyright 2010 alvin j. alexander, devdaily.com.
 * 
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.

 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser Public License for more details.

 * You should have received a copy of the GNU Lesser Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * 
 * Please ee the following page for the LGPL license:
 * http://www.gnu.org/licenses/lgpl.txt
 * 
 */
public class MySystemCommandExecutor{
	
	private class StreamGobbler extends Thread {
		private boolean info; //whether save stdout and err 
		private boolean isPrintToScreen;//whether print stdout and err immediately on screen during binary running
		private String spacer;
		private StringBuffer outputBuffer;
		private InputStream in;

	    private StreamGobbler(InputStream in, boolean info, String spacer, StringBuffer outputBuffer, boolean isPrintToScreen) {
	        this.in=in;
	        this.info = info;
	        this.spacer = spacer;
	        this.outputBuffer = outputBuffer;
	        this.isPrintToScreen=isPrintToScreen;
	    }

	    @Override
	    public void run() {
	        try {
	            BufferedReader br = new BufferedReader(new InputStreamReader(in));
	            String line = null;
	            while ((line = br.readLine()) != null){
	            	if (info) {
		            	outputBuffer.append(spacer+line+"\n");
					}
	            	if (isPrintToScreen) {
						System.out.println(spacer+line);
					}
	            }
	        }catch (IOException ioe) {
	            ioe.printStackTrace();
	        }
	    }
	}
	
	private List<String> commandInformation;
	private boolean info;
	private boolean isPrintToScreen;
	private StringBuffer outputBuffer_std;
	private StringBuffer outputBuffer_err;
	/**
	 * Pass in the system command you want to run as a List of Strings, as shown here:
	 * 
	 * List<String> commands = new ArrayList<String>();
	 * commands.add("/sbin/ping");
	 * commands.add("-c");
	 * commands.add("5");
	 * commands.add("www.google.com");
	 * SystemCommandExecutor commandExecutor = new SystemCommandExecutor(commands);
	 * commandExecutor.executeCommand();
	 * 
	 * Note: I've removed the other constructor that was here to support executing
	 *       the sudo command. I'll add that back in when I get the sudo command
	 *       working to the point where it won't hang when the given password is
	 *       wrong.
	 *
	 * @param commandInformation The command you want to run.
	 */
  
	
  
	public MySystemCommandExecutor(final List<String> commandInformation, boolean info, boolean isPrintToScreen){
		if (commandInformation==null) throw new NullPointerException("The commandInformation is required.");
		this.commandInformation = commandInformation;
		this.info=info;
		this.isPrintToScreen=isPrintToScreen;
		if (info) {
			outputBuffer_err=new StringBuffer();
			outputBuffer_std=new StringBuffer();
		}
	}
  
	@SuppressWarnings("finally")
	public int executeCommand(boolean disp, String spacer, String timeFormat, String usrInput) throws IOException, InterruptedException{
		General.dispInfo_ifNeed(disp, spacer, "... start execut commond:"+commandInformation);
		long startTime=System.currentTimeMillis();
		int exitValue = -99;
		
		try{
//		  	//method 1, for disp all info to the console
//		  	Process process = new ProcessBuilder(commandInformation).redirectError(Redirect.INHERIT).redirectInput(Redirect.INHERIT).start(); 
			//method 2, do not redirect, need a separte thread to drain the output of the binary, otherwise this process will hang and deadlocked!!
			Process process = new ProcessBuilder(commandInformation).start();
	      	// you need this if you're going to write something to the command's input stream (such as when invoking the 'sudo' command, and it prompts you for a password).
			if (usrInput!=null) {
				OutputStream stdOutput = process.getOutputStream();
				stdOutput.write(usrInput.getBytes());
				stdOutput.flush();stdOutput.close();
			}
			//handle outputs info for method 2
			StreamGobbler errorGobbler = new StreamGobbler(process.getErrorStream(), info, spacer+"errout: ", outputBuffer_err, isPrintToScreen);
			StreamGobbler outputGobbler = new StreamGobbler(process.getInputStream(), info, spacer+"stdout: ", outputBuffer_std, isPrintToScreen);
			errorGobbler.start();
			outputGobbler.start();
			//wait process until finished
			exitValue = process.waitFor();
	    }catch (IOException e){
	      throw e;
	    }catch (InterruptedException e){
	      throw e;
	    }finally{
	  	  General.dispInfo_ifNeed(disp, spacer, "... done for execut commond, its excution time:"+General.dispTime(System.currentTimeMillis()-startTime, timeFormat));
	      return exitValue;
	    }
	  }
	
	/**
	 * Get the standard output (stdout) from the command you just exec'd.
	 */
	public String getStandardOutputFromCommand(){
		return outputBuffer_std.toString();
	}

	/**
	 * Get the standard error (stderr) from the command you just exec'd.
	 */
	public String getStandardErrorFromCommand(){
		return outputBuffer_err.toString();
	}
  
	public static void main(String[] args) throws Exception{ //for test
		//**********solution1***************
		// build the system command we want to run
		List<String> commands = Arrays.asList("cmd", "/c", "dir");
//		    commands.add("/bin/sh");
//		    commands.add("-c");
//		    commands.add("ls -l /var/tmp | grep tmp");
    
		// execute the command
		MySystemCommandExecutor commandExecutor = new MySystemCommandExecutor(commands,true,true);
		int result = commandExecutor.executeCommand(true, "","s",null);

		// get the stdout and stderr from the command that was run
		String stdout = commandExecutor.getStandardOutputFromCommand();
		String stderr = commandExecutor.getStandardErrorFromCommand();
    
		// print the stdout and stderr
		System.out.println("The numeric result of the command was: " + result);
		System.out.println("STDOUT:");
		System.out.println(stdout);
		System.out.println("STDERR:");
		System.out.println(stderr);
  }
}







