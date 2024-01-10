pipeline {
    agent any

    tools {
        maven 'myMaven'
    }

    environment {
        SCRIPT_PATH = "/opt/python/microsoft-graph-api/main.py"
        VAULT_URL = "http://hashicorp-vault.bnh.vn:8200/v1/microsoft/graph-api"
    }

    stages {
        stage('Build with Maven') {
            steps {
                sh 'mvn clean package kafka-connect:kafka-connect -DskipTests'
            }
        }
        
        stage('Upload the lastest build') {
            steps {
                script {
                    def VERSION = sh(script: 'mvn help:evaluate -Dexpression=project.version -q -DforceStdout', returnStdout: true).trim()
                    def ASSET_PATH = "target/components/packages/BNHTech-kafka-jdbc-sink-connector-${VERSION}.zip"

                    withCredentials([string(credentialsId: 'vault-token', variable: 'VAULT_TOKEN')]) {
                        sh "python3 ${SCRIPT_PATH} ${VAULT_URL} ${VAULT_TOKEN} ${ASSET_PATH} ${GIT_URL} ${GIT_COMMIT}"
                    }
                }
            }
        }
    }
}