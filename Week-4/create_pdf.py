import pyautogui
import time
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
from reportlab.lib.utils import ImageReader
import os
from datetime import date

def take_screenshots():
    print("Prepare to take screenshots. You have 5 seconds to switch to the relevant window.")
    time.sleep(5)
    
    screenshots = [
        ("model_creation.png", "Switch to your model creation window"),
        ("flask_app.png", "Switch to your Flask app window"),
        ("api_test.png", "Switch to your API testing window")
    ]
    
    for filename, instruction in screenshots:
        print(instruction)
        input("Press Enter when ready to take the screenshot...")
        try:
            pyautogui.screenshot(filename)
            print(f"Screenshot saved as {filename}")
        except Exception as e:
            print(f"Error taking screenshot {filename}: {str(e)}")
        time.sleep(2)

def create_pdf():
    c = canvas.Canvas("deployment_steps.pdf", pagesize=letter)
    width, height = letter

    c.setFont("Helvetica-Bold", 16)
    c.drawString(50, height - 50, "Model Deployment Steps")
    c.setFont("Helvetica", 12)
    c.drawString(50, height - 70, "Name: Amr Hacoglu")
    c.drawString(50, height - 90, "Batch Code: LISUM34")
    c.drawString(50, height - 110, f"Submission Date: {date.today().strftime('%B %d, %Y')}")
    c.drawString(50, height - 130, "Submitted to: Data Glacier")

    screenshots = [
        ("Step 1: Creating the model", "model_creation.png"),
        ("Step 2: Running Flask app", "flask_app.png"),
        ("Step 3: Testing the API", "api_test.png")
    ]

    y_position = height - 180
    for title, image_path in screenshots:
        c.setFont("Helvetica-Bold", 14)
        c.drawString(50, y_position, title)
        if os.path.exists(image_path):
            try:
                img = ImageReader(image_path)
                c.drawImage(img, 50, y_position - 220, width=500, height=200)
            except Exception as e:
                c.drawString(50, y_position - 20, f"Error including image: {str(e)}")
        else:
            c.drawString(50, y_position - 20, f"Image not found: {image_path}")
        y_position -= 250

    c.save()
    print("PDF created successfully.")

if __name__ == "__main__":
    print("Current working directory:", os.getcwd())
    take_screenshots()
    create_pdf()