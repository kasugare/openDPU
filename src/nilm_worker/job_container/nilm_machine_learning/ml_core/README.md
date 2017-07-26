# pyenlearner
Python Encored Learner for deep learning



- Test
  - model fitting
  '''
  ./fit_model.py -n 30 -i /disk1/raw_data_with_plug/dl-by-device/merged_2ch/sample -o ../temp_learner -m 2ch_cnn_lstm -f encompact2chdiffaprp -p 600 -w tv_2ch_cnn_lstm -s 1000 -t self
  '''
  - model evaluation
  '''
  THEANO_FLAGS=device=gpu0,floatX=float32 python eval_model.py -i
  /disk1/raw_data_with_plug/dl-by-device/merged_2ch/jp-F302002E-tv_window-60-sec_v0.2.1.csv,/disk1/raw_data_with_plug/dl-by-device/merged_2ch/jp-F3020053-tv_window-60-sec_v0.2.1.csv,/disk1/raw_data_with_plug/dl-by-device/merged_2ch/jp-F3020026-tv_window-60-sec_v0.2.1.csv
  -p 600 -m 2ch_lstm -f encompact2chdiffaprp -w ../temp_learner/2ch_lstm_F3020073_6 -o ../temp_learner
  THEANO_FLAGS=device=gpu0,floatX=float32 python eval_model.py -i
  /disk1/raw_data_with_plug/dl-by-device-ac/jp-F3020073/data_F3020073_20161129-20161130_00_F3020073.csv,/disk1/raw_data_with_plug/dl-by-device-ac/jp-F3020073/data_F3020073_20161130-20161201_00_F3020073.csv
  -p 600 -m 2ch_cnn_lstm -f encompact2chdiffaprpfromraw -o ../temp_learner -w ../temp_learner/ac_2ch_cnn_lstm_F3020073_2
  '''
