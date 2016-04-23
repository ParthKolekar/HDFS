import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Mapper {

	public String map(String string) throws IOException {
		ArrayList<String> lines = new ArrayList<String>();
		String pattern = "(.*)(" + new String(Files.readAllBytes(Paths.get("Resources/grepword.data"))) + ")(.*)";
		Pattern r = Pattern.compile(pattern);
		for (String line : string.split(System.lineSeparator())) {
			line = line.trim();
			line.concat(System.lineSeparator());
			Matcher m = r.matcher(line);
			if (m.find()) {
				lines.add(line);
			}
		}
		return lines.toString();
	}
}
