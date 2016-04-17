package JobTracker;

public class Job {

	private String reducerName;
	private String mapperName;
	private String inputFile;
	private String outputFile;
	private Integer totalMappers;
	private Integer totalReducers;
	private Integer mapTasksStarted;
	private Integer reduceTasksStarted;
	private Boolean isDone;

	public Job() {
		super();
		this.setReducerName(new String());
		this.setMapperName(new String());
		this.setInputFile(new String());
		this.setOutputFile(new String());
		this.setTotalMappers(0);
		this.setTotalReducers(0);
		this.setMapTasksStarted(0);
		this.setReduceTasksStarted(0);
		this.setIsDone(false);
	}

	public String getInputFile() {
		return this.inputFile;
	}

	public Boolean getIsDone() {
		return this.isDone;
	}

	public String getMapperName() {
		return this.mapperName;
	}

	public Integer getMapTasksStarted() {
		return this.mapTasksStarted;
	}

	public String getOutputFile() {
		return this.outputFile;
	}

	public String getReducerName() {
		return this.reducerName;
	}

	public Integer getReduceTasksStarted() {
		return this.reduceTasksStarted;
	}

	public Integer getTotalMappers() {
		return this.totalMappers;
	}

	public Integer getTotalReducers() {
		return this.totalReducers;
	}

	public void setInputFile(String inputFile) {
		this.inputFile = inputFile;
	}

	public void setIsDone(Boolean isDone) {
		this.isDone = isDone;
	}

	public void setMapperName(String mapperName) {
		this.mapperName = mapperName;
	}

	public void setMapTasksStarted(Integer mapTasksStarted) {
		this.mapTasksStarted = mapTasksStarted;
	}

	public void setOutputFile(String outputFile) {
		this.outputFile = outputFile;
	}

	public void setReducerName(String reducerName) {
		this.reducerName = reducerName;
	}

	public void setReduceTasksStarted(Integer reduceTasksStarted) {
		this.reduceTasksStarted = reduceTasksStarted;
	}

	public void setTotalMappers(Integer totalMappers) {
		this.totalMappers = totalMappers;
	}

	public void setTotalReducers(Integer totalReducers) {
		this.totalReducers = totalReducers;
	}

}
