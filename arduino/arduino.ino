#include <SPI.h>
#include <WiFi.h>
#include <IRremote.hpp>

//#define DEBUG // to see if attachInterrupt is used
//#define TRACE // to see the state of the ISR state machine

/*
 * Protocol selection
 */
//#define DISABLE_PARITY_CHECKS // Disable parity checks. Saves 48 bytes of program memory.
#define USE_EXTENDED_NEC_PROTOCOL // Like NEC, but take the 16 bit address as one 16 bit value and not as 8 bit normal and 8 bit inverted value.
//#define USE_ONKYO_PROTOCOL    // Like NEC, but take the 16 bit address and command each as one 16 bit value and not as 8 bit normal and 8 bit inverted value.
//#define USE_FAST_PROTOCOL     // Use FAST protocol instead of NEC / ONKYO.
//#define ENABLE_NEC2_REPEATS // Instead of sending / receiving the NEC special repeat code, send / receive the original frame for repeat.
/*
 * Set compile options to modify the generated code.
 */
//#define DISABLE_PARITY_CHECKS // Disable parity checks. Saves 48 bytes of program memory.
//#define USE_CALLBACK_FOR_TINY_RECEIVER  // Call the fixed function "void handleReceivedTinyIRData()" each time a frame or repeat is received.

#include "TinyIRReceiver.hpp" // include the code

#define pinIR     15
#define pinPhoto  32
#define pinLED    23

// Wifi Config
//SSID of your network
const char* ssid = "kmalhal-hotspot";
//password of your WPA Network
const char* password = "";

const uint16_t port = 8090;
const char* host = "10.42.0.1";

WiFiClient client;

void setup() {
  Serial.begin(115200);
  delay(100);
  // pinMode(pinIR,    INPUT);
  pinMode(pinPhoto, INPUT);
  pinMode(pinLED,   OUTPUT);
  // while(!Serial) {
  //   delay(100);
  // }
  IrReceiver.begin(pinIR); // Start the receiver
  Serial.println("IR Receive enabled");
  
  WiFi.mode(WIFI_STA);
  WiFi.begin(ssid, password);
  Serial.println("\nConnecting");

  while(WiFi.status() != WL_CONNECTED){
    Serial.print(".");
    delay(500);
  }

  Serial.println("\nConnected to the WiFi network");
  Serial.print("Local ESP32 IP: ");
  Serial.println(WiFi.localIP());

  Serial.println("Connecting to server...");
  /*while (!client.connect(host, port)) {
    Serial.println("Connection to server failed");
    delay(1000);
  }*/
  if (!client.connect(host, port))
    Serial.println("Connection to server failed");
  else Serial.println("Connected to server successful!");
}


void loop() {
  int valLight = analogRead(pinPhoto);
  char light_data[50];
  char ir_data[50];
  // digitalWrite(pinLED, HIGH);
  // Serial.printf("Light: %d\n", valLight);
  if (valLight <= 1500) {
    digitalWrite(pinLED, HIGH);  // analogRead values go from 0 to 1023, analogWrite values from 0 to 255
  } else {
    digitalWrite(pinLED, LOW);
  }

  if (IrReceiver.decode()) {
    // Serial.println(IrReceiver.decodedIRData.command, HEX); // Print "old" raw data
    sprintf(ir_data, "IR: 0x%X%X\n", IrReceiver.decodedIRData.address, IrReceiver.decodedIRData.command); // Add newline at the end
    Serial.printf("IR Protocol:%s, IR Code: %s", IrReceiver.getProtocolString(), ir_data);
    // IrReceiver.printIRResultShort(&Serial); // Print complete received data in one line
    // IrReceiver.printIRSendUsage(&Serial);   // Print the statement required to send this data
    IrReceiver.resume(); // Enable receiving of the next value
  }
  /* if (client.connected()) {
    if (IrReceiver.decode()) {
      client.print(ir_data);    // Send IR data
      delay(100);
    }
    sprintf(light_data, "light: %d\n", valLight); // Add newline at the end
    client.print(light_data); // Send light data
  }
  else {
    client.stop();
  }*/
  delay(200);
}
