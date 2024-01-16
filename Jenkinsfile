pipeline {
    agent any

    tools {
        maven 'myMaven'
    }

    environment {
        BUILD_SCRIPT_PATH = "/opt/python/microsoft-graph-api/main.py"
        ALERT_SCRIPT_PATH = "/opt/python/microsoft-graph-api/alert.py"
        VAULT_URL = "http://hashicorp-vault.bnh.vn:8200/v1/microsoft/graph-api"
        JENKINS_URL = "http://jenkins.gitlab.bnh.vn"
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
                    def ASSET_NAME = "BNHTech-kafka-jdbc-sink-connector-${VERSION}.zip"
                    def GIT_LOG = sh(script: "git log -1 --format=%B", returnStdout: true).trim()

                    withCredentials([string(credentialsId: 'vault-token', variable: 'VAULT_TOKEN')]) {
                        sh """
                            python3 ${BUILD_SCRIPT_PATH} \
                            ${VAULT_URL} ${VAULT_TOKEN} \
                            ${ASSET_PATH} ${GIT_URL} \
                            ${GIT_COMMIT} ${ASSET_NAME} \
                            "${GIT_LOG}"
                        """
                    }
                }
            }
        }
    }

    post {
        always {
            cleanWs()
        }
        
        failure {
            script {
                withCredentials([string(credentialsId: 'vault-token', variable: 'VAULT_TOKEN')]) {
                    sh "python3 ${ALERT_SCRIPT_PATH} ${VAULT_URL} ${VAULT_TOKEN} ${JOB_NAME} ${JENKINS_URL} ${BUILD_ID}"
                }
            }
        }
    }
}