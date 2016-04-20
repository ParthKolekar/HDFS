import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Mapper {

	public String map(String string) throws IOException {
		String[] splitStr = string.split(System.lineSeparator());
		ArrayList<String> lines = new ArrayList<String>();
		String tofind = new String(Files.readAllBytes(Paths.get("Resources/grepword.data")));
		String pattern = "(.*)(" + tofind + ")(.*)";
		Pattern r = Pattern.compile(pattern);
		for (String line : splitStr) {
			line = line.trim();
			Matcher m = r.matcher(line);
			if (m.find()) {
				lines.add(line);
				lines.add(System.lineSeparator());
			}
		}
		System.out.println(lines.toString());
		return lines.toString();
	}
}
