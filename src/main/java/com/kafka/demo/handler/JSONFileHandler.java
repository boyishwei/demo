package com.kafka.demo.handler;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.kafka.demo.vo.TradeMessage;
import lombok.Data;
import org.json.JSONObject;
import org.testcontainers.shaded.org.apache.commons.io.FileUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.FileUrlResource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.stereotype.Component;

import javax.imageio.stream.FileImageOutputStream;
import java.io.*;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

@Data
@Component
public class JSONFileHandler {
    @Value("classpath:${input.json.file}")
    private String fileLocation;

    @Value("fileLocation")
    private File inputFile;

    @Autowired
    private ResourceLoader resourceLoader;

    @Autowired
    ObjectMapper objectMapper;

    TypeReference<List<TradeMessage>> typeReference = new TypeReference<List<TradeMessage>>(){};

    public List<TradeMessage>read() throws IOException {

        InputStream inputStream = resourceLoader.getResource(fileLocation).getInputStream();

        try {
            List<TradeMessage> messages = objectMapper.readValue(inputStream,typeReference);
            return  messages;
        } catch (JsonMappingException e) {
            e.printStackTrace();
        } catch (JsonParseException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public void archive() {
        File file = this.getInputFile();
        try {
            ZipOutputStream zipOutput = new ZipOutputStream(new BufferedOutputStream(new FileOutputStream(file.getAbsolutePath().substring(0, file.getAbsolutePath().lastIndexOf(".")) + LocalDateTime.now() + ".zip")));
            ZipEntry zEntry = new ZipEntry(file.getName());
            zipOutput.putNextEntry(zEntry);
            BufferedInputStream bis = new BufferedInputStream(new FileInputStream(file));
            byte[] buffer = new byte[1024];
            int read = 0;
            while((read = bis.read(buffer)) != -1){
                zipOutput.write(buffer, 0, read);
            } bis.close(); } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public void writeToJsonFile(File file, String json) throws IOException {
        JsonGenerator generator =  objectMapper.getFactory().createGenerator(new FileImageOutputStream(file));
        TradeMessage message = objectMapper.readValue(json, TradeMessage.class);
        List<TradeMessage> messages;

        if(!file.exists())
            file.createNewFile();

        if(file.length() !=0)
            messages = objectMapper.readValue(new FileInputStream(file),typeReference);
        else
            messages = new ArrayList<>();

        messages.add(message);
        objectMapper.writerWithDefaultPrettyPrinter().writeValue(file,messages);
        generator.close();
    }
}
