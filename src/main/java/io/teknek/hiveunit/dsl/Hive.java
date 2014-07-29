package io.teknek.hiveunit.dsl;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.teknek.hiveunit.client.HiveClient;
import io.teknek.hiveunit.common.Response;
import io.teknek.hiveunit.common.ResponseStatus;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Provides means to execute scripts against a HiveClient
 * and retrieve responses. May be used generically,
 * not just for testing purposes.
 */
public class Hive {
    private HiveClient hive;
    private String scriptFile;
    private Map<String, String> params;

    protected Hive(HiveClient hive, String scriptFile) {
        this(hive, scriptFile, Maps.<String, String>newHashMap());
    }

    protected Hive(HiveClient hive, String scriptFile, Map<String, String> params) {
        this.hive = hive;
        this.scriptFile = scriptFile;
        this.params = params;
    }

    public Response execute() {
        Response response = null;
        List<String> out = Lists.newArrayList();
        for (String command : buildCommandsFromScript()) {
            response = hive.execute(replaceVariables(command));
            out.addAll(response.getResult());
        }
        return new Response((response == null) ? ResponseStatus.FAILURE : response.getResponseStatus(), out);
    }

    private String replaceVariables(String line) {
        StringBuilder newLine = new StringBuilder();
        String[] tokens = line.split(" ");
        for (int j = 0; j < tokens.length; j++) {
            for (String param : params.keySet()) {
                if (tokens[j].equals(param)) {
                    tokens[j] = params.get(param);
                    break;
                } else if (tokens[j].equals(String.format("'%s'", param))) {
                    tokens[j] = String.format("'%s'", params.get(param));
                    break;
                }
            }
            newLine.append(tokens[j]).append(" ");
        }
        return newLine.toString();
    }

    private List<String> buildCommandsFromScript() {
        List<String> commands = Lists.newArrayList();
        BufferedReader in;
        try {
            in = new BufferedReader(new FileReader(scriptFile));
            String line;
            StringBuilder command = new StringBuilder();
            while ((line = in.readLine()) != null) {
                if (!line.isEmpty() && !line.startsWith("--")) {
                    //TODO consider case of multiple commands in single line
                    command.append(line);
                    if (line.endsWith(";")) {
                        commands.add(replaceVariables(command.toString().replace(";", "")));
                        command = new StringBuilder();
                    }
                }
            }
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return commands;
    }
}
