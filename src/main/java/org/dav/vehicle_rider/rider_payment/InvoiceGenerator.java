package org.dav.vehicle_rider.rider_payment;

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.apache.commons.io.IOUtils;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.pdmodel.PDPageContentStream;
import org.apache.pdfbox.pdmodel.font.PDType0Font;
import org.apache.pdfbox.pdmodel.graphics.image.PDImageXObject;
import org.dav.vehicle_rider.EndRideFlow;
import org.dav.vehicle_rider.messages.VehicleMessageWithPaymentMethod;
import org.slf4j.Logger;

import java.awt.*;
import java.io.*;
import java.math.BigDecimal;
import java.math.MathContext;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Currency;
import java.util.Date;
import java.util.UUID;

public class InvoiceGenerator implements Serializable, VehicleMessageWithPaymentMethod {

//    public static void main(String[] args) {
//        InvoiceGenerator i = new InvoiceGenerator();
//        i.generateInvoice();
//    }

    private static final long serialVersionUID = 1L;

    private Logger log;
    private UUID riderId;
    private String paymentMethodId;
    private String companyName;
    private Date date;
    private BigDecimal distance;
    private long durationMS;
    private String cardBrand;
    private String last4Digits;
    private String currencyCode;
    private String gcsBucket;
    private BigDecimal totalPrice;
    private BigDecimal basePrice;
    private BigDecimal pricePerMinute;
    private boolean isPaymentMethodDAV;

    public InvoiceGenerator(EndRideFlow.EndRideMessage endRideMessage, Logger log, String gcsBucket) {
        this.log = log;
        this.riderId = endRideMessage.getRiderId();
        this.companyName = endRideMessage.getCompanyName();
        this.date = endRideMessage.getLastTime();
        this.currencyCode = endRideMessage.getFiatCurrencyCode();
        this.basePrice = endRideMessage.getBasePrice();
        this.pricePerMinute = endRideMessage.getPricePerMinute();
        this.totalPrice = endRideMessage.getRidePrice();
        this.distance = endRideMessage.getRideDistance();
        this.gcsBucket = gcsBucket;
        this.durationMS = date.getTime() - endRideMessage.getStartTime().getTime();
        this.isPaymentMethodDAV = endRideMessage.isPaymentMethodDAV();
    }

//    public InvoiceGenerator() {
//        this.companyName = "Hai";
//        this.date = new Date();
//        this.currencyCode = "USD";
//        this.basePrice = new BigDecimal(1);
//        this.pricePerMinute = new BigDecimal(1);
//        this.totalPrice = new BigDecimal(2);
//        this.distance = new BigDecimal(2300);
//        this.gcsBucket = "";
//        this.durationMS = 230000;
//        this.isPaymentMethodDAV = true;
//        this.last4Digits = "1234";
//        this.cardBrand = "VISA";
//    }

    public File convertInputStreamToFile(InputStream is) throws IOException
    {
        OutputStream outputStream = null;
        File file = new File("./asset");
        try
        {
            outputStream = new FileOutputStream(file);

            int read = 0;
            byte[] bytes = new byte[1024];
            while ((read = is.read(bytes)) != -1) {
                outputStream.write(bytes, 0, read);
            }
        }
        finally
        {
            if(outputStream != null)
            {
                outputStream.close();
            }
        }
        return file;
    }

    public UUID getRiderId() {
        return this.riderId;
    }

    public void setPaymentMethodId(String paymentMethodId) {
        this.paymentMethodId = paymentMethodId;
    }

    public String getPaymentMethodId() {
        return this.paymentMethodId;
    }

    public void setPaymentInfo(String cardBrand, String last4Digits) {
        this.cardBrand = cardBrand;
        this.last4Digits = last4Digits;
    }

    public Void generateInvoice() {
        try {
            log.info(String.format("start generate an invoice for riderId: %s date: %s paymentMethodDAV: %s", this.riderId, this.date, this.isPaymentMethodDAV));
            ClassLoader classLoader = getClass().getClassLoader();
            String pdfTemplateName = isPaymentMethodDAV ? "invoice_template_dav.pdf" : "invoice_template.pdf";
            InputStream stream = classLoader.getResourceAsStream(pdfTemplateName);
            PDDocument document = PDDocument.load(stream);

            PDPage page = document.getPage(0);

            String currency;
            try {
                currency = Currency.getInstance(currencyCode).getSymbol();
            } catch (Exception e) {
                currency = currencyCode;
            }
            MathContext roundContext = new MathContext(2);
            PDPageContentStream contentStream = new PDPageContentStream(document, page, PDPageContentStream.AppendMode.APPEND, true);
            PDType0Font montserratRegular = PDType0Font.load(document, classLoader.getResourceAsStream("Montserrat-Regular.ttf"));
            PDType0Font montserratBold = PDType0Font.load(document, classLoader.getResourceAsStream("Montserrat-Bold.ttf"));

            contentStream.beginText();

            //Title
            contentStream.setFont(montserratBold, 24);
            contentStream.setNonStrokingColor(Color.black);
            String title = String.format("Your DAV ride with %s", companyName);
            int fontSize = 24;
            float titleWidth = montserratBold.getStringWidth(title) / 1000 * fontSize;
            float centerMargin = (page.getMediaBox().getWidth() - titleWidth) / 2;
            contentStream.newLineAtOffset(centerMargin, 430);
            contentStream.showText(title);

            contentStream.endText();
            contentStream.beginText();

            //Date
            contentStream.setFont(montserratRegular, 16);
            contentStream.setNonStrokingColor(Color.black);
            contentStream.newLine();
            contentStream.newLineAtOffset(60, 352);
            DateFormat df = new SimpleDateFormat("dd/MM/yy, HH:mm");
            String dateString = df.format(date);
            contentStream.showText(dateString);

            //Distance
            if (distance == null) {
                distance = new BigDecimal(0);
            }
            contentStream.newLine();
            contentStream.newLineAtOffset(152, 0);
            String distanceString = String.format("%skm", distance.round(roundContext).toPlainString());
            contentStream.showText(distanceString);

            //Duration
            long duration = durationMS / 1000;
            contentStream.newLine();
            contentStream.newLineAtOffset(140, 0);
            String durationString = String.format("%d:%d mins", duration / 60, duration % 60);
            contentStream.showText(durationString);

            //Credit card last 4 digits
            contentStream.newLine();
            contentStream.newLineAtOffset(160, 0);
            contentStream.showText(last4Digits);

            contentStream.newLine();
            contentStream.newLineAtOffset(-440, -66);
            contentStream.showText(String.format("%s ride", companyName));

            String price = String.format("%s%s", currency, totalPrice.round(roundContext).toPlainString());
            String priceDescription = String.format("%s%s for unlock + %s%s Per minute ",
                    currency, basePrice.round(roundContext).toPlainString(),
                    currency, pricePerMinute.round(roundContext).toPlainString() );
            contentStream.newLine();
            contentStream.setNonStrokingColor(Color.decode("#686868"));
            contentStream.newLineAtOffset(452, 0);

            try {
                contentStream.setFont(montserratBold, 16);
                contentStream.showText(price);
                contentStream.newLine();
                if (isPaymentMethodDAV) {
                    contentStream.newLineAtOffset(-12, -54);
                    contentStream.showText("- " + price);
                    contentStream.newLineAtOffset(-440, 32);
                } else  {
                    contentStream.newLineAtOffset(0, -74);
                    contentStream.showText(price);
                    contentStream.newLine();
                    contentStream.newLineAtOffset(-450, 52);
                }

                contentStream.setNonStrokingColor(Color.decode("#A0A0A0"));
                contentStream.setFont(montserratRegular, 12);
                contentStream.showText(String.format(priceDescription));
            } catch (Exception e) {

                PDType0Font symbols = PDType0Font.load(document, classLoader.getResourceAsStream("FreeSerifBold.ttf"));
                contentStream.setFont(symbols, 16);
                contentStream.showText(price);

                contentStream.newLine();
                if (isPaymentMethodDAV) {
                    contentStream.newLineAtOffset(-10, -54);
                    contentStream.showText("- " + price);
                    contentStream.newLineAtOffset(-440, 32);
                } else  {
                    contentStream.newLine();
                    contentStream.newLineAtOffset(0, -74);
                    contentStream.showText(price);
                    contentStream.newLineAtOffset(-450, 52);
                }

                contentStream.setNonStrokingColor(Color.decode("#A0A0A0"));
                contentStream.setFont(symbols, 12);
                contentStream.showText(String.format(priceDescription));
            }

            contentStream.endText();

            //Credit card logo
            try {
                String fileName = String.format("%s.png", cardBrand.toLowerCase());
                PDImageXObject pdImage = PDImageXObject.createFromFileByContent(convertInputStreamToFile(classLoader.getResourceAsStream(fileName)), document);
                contentStream.drawImage(pdImage, 484, 350);
            } catch (Exception ex) {
                log.error(String.format("Fail to set card logo for riderId: %s, date: %s", this.riderId, this.date), ex);
            }

            contentStream.close();
//            document.save("./sample.pdf");
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            document.save(byteArrayOutputStream);

            InputStream inputStream = new ByteArrayInputStream(byteArrayOutputStream.toByteArray());
            Storage storage = StorageOptions.getDefaultInstance().getService();
            DateFormat namingFormat = new SimpleDateFormat("yy-MM-dd_HH-mm_");
            BlobId blobId = BlobId.of(gcsBucket, namingFormat.format(date) + riderId + ".pdf");
            BlobInfo blobInfo = BlobInfo.newBuilder(blobId).setContentType("application/pdf").build();
            storage.create(blobInfo, IOUtils.toByteArray(inputStream));

            document.close();

        } catch (Exception ex) {
            log.error(String.format("Fail to create invoice for riderId: %s, date: %s, error: %s", this.riderId, this.date, ex.toString()));
        }
        return null;
    }
}
