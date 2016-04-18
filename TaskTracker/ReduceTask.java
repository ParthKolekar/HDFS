package TaskTracker;

import java.util.List;
import java.util.concurrent.Callable;

class ReduceTask implements Callable<String[]> {

	private String taskID;

	public ReduceTask(List<String> mapOutputFilesList, String reducerName, Integer taskID) {
		this.taskID = Integer.toString(taskID);
	}

	@Override
	public String[] call() throws Exception {
		String arr[] = new String[2];
		arr[0] = this.taskID;
		arr[1] = "Map";
		return arr;
	}

}