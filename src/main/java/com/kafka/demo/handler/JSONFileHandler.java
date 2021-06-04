package com.kafka.demo.handler;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.demo.vo.TradeMessage;
import lombok.Data;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ResourceLoader;

import java.io.*;
import java.time.LocalDateTime;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

@Data
public class JSONFileHandler {
    @Value("classpath:${input.json.file}")
    private String fileLocation;

    @Value("fileLocation")
    private File inputFile;

    @Autowired
    private ResourceLoader resourceLoader;
    public List<TradeMessage>read() throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        TypeReference<List<TradeMessage>> typeReference = new TypeReference<List<TradeMessage>>(){};

        InputStream inputStream = resourceLoader.getResource(fileLocation).getInputStream();

        try {
            List<TradeMessage> messages = mapper.readValue(inputStream,typeReference);
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
            }
            bis.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
