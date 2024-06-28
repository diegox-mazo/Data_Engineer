import smtplib
from airflow.models import Variable

# Constantes Email
password= Variable.get("secret_pass_email")
smtp_server = 'smtp.gmail.com'
smtp_port = 587
sender_email = 'usb.prueba2020@gmail.com'
recipient_email = 'usb.prueba2020@gmail.com'
subject = 'Carga de datos'
body_text = 'Los datos fueron cargados a la base de datos exitosamente.'

#Envio de email
def send_email():
    try:
        x=smtplib.SMTP(smtp_server, smtp_port)
        x.starttls()
        x.login(sender_email, password)
        subject=subject
        body_text=body_text
        message='Subject: {}\n\n{}'.format(subject,body_text)
        x.sendmail(sender_email, recipient_email, message)
        print('El email fue enviado correctamente.')
    except Exception as exception:
        print(exception)
        print('El email no se pudo enviar.')