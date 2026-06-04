import cv2
import time
import os

print("Checking video files:")
for f in ["CAM 1.mp4", "CAM 2.mp4", "CAM 3.mp4", "CAM 4.mp4", "CAM 5.mp4"]:
    print(f"- {f}: exists={os.path.exists(f)}, size={os.path.getsize(f) if os.path.exists(f) else 'N/A'}")

cap = cv2.VideoCapture("CAM 1.mp4")
print("VideoCapture opened:", cap.isOpened())
if cap.isOpened():
    ret, frame = cap.read()
    print("Frame read success:", ret)
    if ret:
        print("Frame shape:", frame.shape)
    cap.release()

try:
    from ultralytics import YOLO
    print("Ultralytics imported successfully")
    model = YOLO("yolo11n.pt")
    print("Model loaded successfully")
except Exception as e:
    print("Error loading YOLO model:", e)
