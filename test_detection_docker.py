import sys
import traceback

def run_test():
    try:
        import cv2
        import os
        from ultralytics import YOLO

        model = YOLO("yolo11n.pt")
        for i in range(1, 6):
            path = f"CAM {i}.mp4"
            if not os.path.exists(path):
                print(f"{path} does not exist in container!")
                continue
            cap = cv2.VideoCapture(path)
            if not cap.isOpened():
                print(f"Could not open {path} in container!")
                continue
            
            total_frames = 0
            detections = 0
            max_people = 0
            total_length = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
            
            # Sample 1 frame every 30 frames
            step = 30
            for fidx in range(0, total_length, step):
                cap.set(cv2.CAP_PROP_POS_FRAMES, fidx)
                ret, frame = cap.read()
                if not ret:
                    break
                total_frames += 1
                res = model(frame, verbose=False)[0]
                people_in_frame = 0
                for box in res.boxes:
                    if int(box.cls[0]) == 0 and float(box.conf[0]) >= 0.4:
                        people_in_frame += 1
                        detections += 1
                max_people = max(max_people, people_in_frame)
            cap.release()
            print(f"File {path} (length {total_length} frames): Sampled {total_frames} frames. Total person detections: {detections}. Max people in single frame: {max_people}")
    except Exception as e:
        with open("err.txt", "w") as f:
            f.write(str(e) + "\n")
            traceback.print_exc(file=f)
        print("CRASHED:", str(e))
        sys.exit(1)

if __name__ == "__main__":
    run_test()

