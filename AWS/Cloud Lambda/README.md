## Rekognition’s Pre-Trained Model
We started by using the script provided in the AWS Worker Safety project, which focuses on detecting helmets only. After further development, we were able to adapt the code so that the model could detect multiple PPEs at the same time.
While performing the preliminary tests, it was noted that facemasks were not part of the Rekognition’s object catalog, and safety vests were rarely detected. It was assumed that the model had limited predictive power compared to results for safety helmets and boots. 
* Reference URL: https://github.com/aws-samples/aws-deeplens-worker-safety-project
## Rekognition’s Custom Labels Model
Creation of custom labels for safety helmets, vests, and facemasks required the creation of labeling jobs in AWS SageMaker Ground Truth and train hundreds of images in Rekognition (Refer to Appendix 2). Unlike Rekognition’s pre-trained models, a custom label model required manual start and stop of the service via a command-line interface before the execution of the Lambda function. 
Result: The latest script version was now capable of not only recognizing PPEs from the pre-trained model but also those from the customized labels that were obtained that this stage.
