package TaskTracker;

import java.util.concurrent.Callable;

class MapTask implements Callable<String[]> {

	private Integer blockID;
	private String mapperName;
	private String taskID;

	public MapTask(Integer blockID, String mapperName, Integer taskID) {
		this.blockID = blockID;
		this.mapperName = mapperName;
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
