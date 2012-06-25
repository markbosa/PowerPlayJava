package etlMain;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * FILE Persistence binding: DOA_FILE
 */

public class FileUtils {
	public final static int LINUX = 0;
	public final static int WINDOWS = 1;
	private int operatingSystem = WINDOWS;
	
	private String baseDir = "";
	private String fileName = "";
	private String filePathName = "";
	private String inFile = "";
	private String outFile = "";
	
	private FileInputStream fstreamIn = null;
	private BufferedReader bReader  = null;
	private DataInputStream dis = null;

	private BufferedWriter bufferedWriter = null;
//	private BufferedWriter bWriter  = null;
	private DataOutputStream dos = null;

	/**
	 * @return the operatingSystem
	 */
	public int getOperatingSystem() {
		return operatingSystem;
	}
	/**
	 * @param operatingSystem choose the operatingSystem
	 */
	public void setOperatingSystem(int operatingSystem) {
		this.operatingSystem = operatingSystem;
	}
	/**
	 * Get file access parameters
	 */
	public String getFileName( ) {
		return this.fileName;
	}
	/**
	 * Set the File directory path
	 */
	public String getFilePathName( ) {
		return this.filePathName;
	}
	/**
	 * get the opened full file name for batch & easy access
	 * @return String containing path and file name
	 */
	public String getInFile() {
		return inFile;
	}
	/**
	 * @return the buffered Reader
	 */
	public BufferedReader getBufferedInReader() {
		return bReader;
	}

	/**
	 * @return the out
	 */
	public DataOutputStream getOut() {
		return dos;
	}
	/**
	 * @return the fstreamOut
	 */
	public BufferedWriter getFstreamOut() {
		return bufferedWriter;
	}
	/**
	 * @return the writer instance
	 */
	public BufferedWriter getBufferedWriter() {
		return bufferedWriter;
	}
	/**
	 * @return the outFile
	 */
	public String getOutFile() {
		return outFile;
	}
	/**
	 * @param outFile the outFile to set
	 */
	public void setOutFile(String outFile) {
		this.outFile = outFile;
	}

	/**
	 * @param save bufferedWriter value
	 */
	public void setBufferedWriter(BufferedWriter writerOut) {
		this.bufferedWriter = writerOut;
	}
	/**
	 * @param out the out to set
	 */
	public void setOut(DataOutputStream out) {
		this.dos = out;
	}
	/**
	 * Set file access parameters
	 */
	public void setFileName( String fileName ) {
		this.fileName = fileName;
	}
	/**
	 * Set the File directory path
	 */
	public void setFilePathName(String filePathName) {
		this.filePathName = filePathName;
	}
	/**
	 * Set the full file and path name for easy access
	 * @return
	 */
	public void setInFile(String smtFile) {
		this.inFile = smtFile;
	}
	/**
	 * @param br the br to set
	 */
	public void setBufferedReader(BufferedReader br) {
		this.bReader = br;
	}
	/**
	 * Initialize the buffered output writer
	 * NOTE: The filename MUST be set first before calling this method
	 */
	public void initBufferedWriter() {
		//Construct the BufferedWriter object
		try {
			this.setBufferedWriter(new BufferedWriter(new FileWriter(this.getFileName())));
		} catch (IOException e) {
			System.out.println("initBufferedWriter:: Failed opening Filename: " + this.getFileName());
			e.printStackTrace();
		}
	}
	/**
	 * @return the baseDir
	 */
	public String getBaseDir() {
		return baseDir;
	}

	/**
	 * @param baseDir the baseDir to set
	 */
	public void setBaseDir(String baseDir) {
		this.baseDir = baseDir;
	}
	/**
	 * settOS() - Detect the operating system
	 */
	void setOS() {

		String os = System.getProperty("os.name").toLowerCase();

		if(os.indexOf("win") >= 0)				// windows or Linux?
			setOperatingSystem(WINDOWS);		
		else if(os.indexOf("nix") >= 0 || os.indexOf("nux") >= 0)			
			setOperatingSystem(LINUX);
		else
			setOperatingSystem(LINUX);
	}
	/**
	 * Open the file for read access
	 * Prerequisites:
	 *   1) File Path Name MUST be set
	 *   2) File Name MUST be set to a file located on the File PATH
	 */
	public boolean openFile() {
		boolean rc = false;
		this.setInFile(this.filePathName.concat(this.fileName));

		try{
			// Open the file and save the stream references as instance data
			this.fstreamIn = new FileInputStream(this.inFile);

			// Get the object of DataInputStream
			this.dis = new DataInputStream(fstreamIn);
			this.setBufferedReader(new BufferedReader(new InputStreamReader(dis)));

			rc = true;
		}catch (Exception e){				//Catch exception if any
			System.err.println("Error: " + e.getMessage());
		}
		return rc;
	}

	/**
	 * Open the file for write access
	 * Prerequisites:
	 *   1) File Path Name MUST be set
	 *   2) File Name MUST be set to a file located on the File PATH
	 */
	public boolean openFileWrite() {
		boolean rc = false;
		String fullFileName = this.filePathName.concat(this.fileName);
		
		File directory = new File(this.filePathName);
		
		boolean exists = (directory).exists();
		if (exists) {
			// File or directory exists
			System.out.println("Directory [" + this.filePathName + "] exists");
		} else if( directory.mkdirs()) 
			System.out.println("Directory [" + this.filePathName + "] created as it did not exist" );
		
		this.setOutFile(fullFileName);

		try{
			// Open the file and save the stream references as instance data
			this.bufferedWriter = new BufferedWriter(new FileWriter(this.outFile));

			// Get the object of DataOutputStream
			//this.dos = new DataOutputStream(this.bufferedWriter);

			rc = true;
		}catch (Exception e){				//Catch exception if any
			System.err.println("Error: " + e.getMessage());
		}
		return rc;
	}
	
    /**
     * Writes a line of data to a file using a BufferedWriter
     */
    public void writeToFile(String outLine) {
        
        BufferedWriter bufferedWriter = this.getBufferedWriter();
        
        try {            
            //Start writing to the output stream
            bufferedWriter.write(outLine);
            bufferedWriter.newLine();
            
        } catch (FileNotFoundException ex) {
            ex.printStackTrace();
        } catch (IOException ex) {
            ex.printStackTrace();
        } 
    }
    
    /**
     * After all file writing operations are complete, close it.
     */
    public void closeBufferedWriter() {
            //Close the BufferedWriter
            try {
            	
                if (this.getBufferedWriter() != null) {
                	this.getBufferedWriter().flush();
                	this.getBufferedWriter().close();
                }
            } catch (IOException ex) {
                ex.printStackTrace();
            }
    }
    
	public void closeFileStream() {
		//Close the input stream
		try {
			this.dis.close();
		} catch (IOException e) {
			System.out.println ("IOException closing file: " + this.fileName);				
			e.printStackTrace();
		}
	}
}
