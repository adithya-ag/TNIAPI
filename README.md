# Telecom Network Inventory Management (TNI) - Python Flask Application

## Table of Contents

- [Introduction](#introduction)
- [Project Description](#project-description)
- [Features](#features)
- [Requirements](#requirements)
- [Usage](#usage)
- [License](#license)
- [Acknowledgments](#acknowledgments)

## Introduction

Welcome to the Telecom Network Inventory Management (TNI) Python Flask application for duplicate data detection. This repository contains the Flask component of our Telecom Network Inventory Management project, which complements the Java Spring Boot application. This Flask application is responsible for identifying duplicate records in CSV files sent from the Java side. It uses the Dedupe library to perform the duplicate detection.

The core functionality of this Flask app involves processing CSV files, creating training data for Dedupe, and performing duplicate record identification. It communicates with the Java application via HTTP requests and responds with the identified duplicate data.

## Project Description

The Python Flask application in this project serves as a crucial component for identifying duplicate records in CSV files. Here's how it works:

1. **Receiving Data**: The Flask app receives a POST request from the Java side, including the path to a CSV file generated in the Java app.

2. **Data Processing**: It reads the CSV file and checks if a training file already exists. If not, it goes through a process of interactively asking the user questions to create a training file for Dedupe.

3. **Duplicate Detection**: The app uses Dedupe to compare records in the CSV file and identifies duplicates based on learned attributes and values.

4. **User Interaction**: If necessary, the app may prompt the user to confirm whether attributes of certain records are the same or different while the traing is ongoing.

5. **Training Data Update**: After identifying duplicates, it updates the training data for future use.

6. **Response to Java App**: The identified duplicate data is sent back to the Java application.

## Features

- Duplicate data detection using Dedupe library.
- Interactive user prompts for attribute similarity confirmation.
- Training data generation for Dedupe.
- Seamless communication with the Java Spring Boot application.
- HTTP-based data exchange.

## Requirements

To run the Flask application, you need the following:

- Python 3.x
- Flask
- Dedupe


## Usage

1. Clone this repository to your local machine.

2. Install the required dependencies using the pip install command or if you are using pycharm,then just go to the imports section and on the errored imports click and press 'alt+enter'.

3. Run the Flask application and ensuer that the tranning files and the csv file is downloded in the same path of the flask app.

4. Ensure the Java Spring Boot application is running and can send POST requests to this Flask app.

5. Start sending POST requests from the Java side with the path to the CSV file for duplicate detection.



## License

This project is licensed under the [MIT License](LICENSE).

## Acknowledgments
I would like to express my sincere gratitude to [Sincera Consultancy](https://www.sincera.in/), the company where the I completed my internship. The guidance, support, and real-world experience gained during this internship have been invaluable to me in the development of the Telecom Network Inventory Management (TNI) project.

Special thanks to the mentors who provided invaluable insights and guidance throughout the project:
- [Srivastsa G](https://www.linkedin.com/in/gorursrivatsa/): Srivastsa G's guidance were instrumental in shaping the architecture and design of the TNI project.

- [Kamal](https://www.linkedin.com/in/kamal-nath-tiwari-61143a67/): Kamal's contributions this project on the flask side is signifcant, particularly in the areas of AI and Machine Learning integration,and as a  guid for Flask API integration significantly enhanced the application's functionality.

- [Anand GP](https://www.linkedin.com/in/anand-gp-58963b26/): Anand GP's dedication and attention to detail in inventory management have greatly improved the project's data accuracy and efficiency.
 
