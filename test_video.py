import cv2
import os

def check_video():
    video_path = r"C:\Users\pushp\OneDrive\Desktop\Purple_Hackathon\CAM 1.mp4"
    print("Checking file exists:", os.path.exists(video_path))

    for backend_name, backend_id in [("DEFAULT", None), ("FFMPEG", cv2.CAP_FFMPEG), ("MSMF", cv2.CAP_MSMF), ("DSHOW", cv2.CAP_DSHOW)]:
        try:
            if backend_id is None:
                cap = cv2.VideoCapture(video_path)
            else:
                cap = cv2.VideoCapture(video_path, backend_id)
            print(f"Backend {backend_name} isOpened:", cap.isOpened())
            if cap.isOpened():
                ret, frame = cap.read()
                print(f"Backend {backend_name} Read success:", ret)
                if ret:
                    print(f"Backend {backend_name} shape:", frame.shape)
            cap.release()
        except Exception as e:
            print(f"Backend {backend_name} error:", e)

if __name__ == "__main__":
    check_video()

