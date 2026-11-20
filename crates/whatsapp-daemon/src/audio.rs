//! Microphone + speaker for one call.
//!
//! The WhatsApp media engine wants 60 ms frames of 960 mono i16 samples at 16 kHz and hands back
//! the same format. cpal gives us whatever the sound server runs at, so this module converts.
//!
//! cpal streams are `!Send`, so they live on one dedicated thread. Dropping `Audio` ends that
//! thread, which drops the streams and releases the devices.

use cpal::traits::{DeviceTrait, HostTrait, StreamTrait};
use cpal::Sample;
use cpal::{SampleFormat, SampleRate, Stream, StreamConfig};
use std::collections::VecDeque;
use std::sync::mpsc;

pub const RATE: u32 = 16_000;
pub const FRAME: usize = 960;
/// Never let playout queue more than this much audio (latency bound): 400 ms.
const MAX_PLAYOUT_SAMPLES: usize = (RATE as usize) * 4 / 10;

pub struct Audio {
    /// Hand to `.audio(mic, speaker)`: frames captured from the microphone.
    pub mic: async_channel::Receiver<Vec<i16>>,
    /// Hand to `.audio(mic, speaker)`: frames to play on the speaker.
    pub speaker: async_channel::Sender<Vec<i16>>,
    _stop: mpsc::Sender<()>,
}

impl Audio {
    pub fn start() -> Result<Audio, String> {
        let (mic_tx, mic_rx) = async_channel::bounded::<Vec<i16>>(3);
        let (spk_tx, spk_rx) = async_channel::bounded::<Vec<i16>>(8);
        let (stop_tx, stop_rx) = mpsc::channel::<()>();
        let (ready_tx, ready_rx) = mpsc::channel::<Result<(), String>>();

        std::thread::Builder::new()
            .name("call-audio".into())
            .spawn(move || {
                let built = (|| -> Result<(Stream, Stream), String> {
                    let host = cpal::default_host();
                    let input = host.default_input_device().ok_or("no microphone found")?;
                    let output = host.default_output_device().ok_or("no speaker found")?;
                    Ok((build_input(&input, mic_tx)?, build_output(&output, spk_rx)?))
                })();
                match built {
                    Ok((input, output)) => {
                        let started = input.play().map_err(|e| format!("microphone: {e}"))
                            .and_then(|_| output.play().map_err(|e| format!("speaker: {e}")));
                        let ok = started.is_ok();
                        let _ = ready_tx.send(started);
                        if ok {
                            // Park until `Audio` is dropped (sender gone => recv returns Err).
                            let _ = stop_rx.recv();
                        }
                        drop((input, output));
                    }
                    Err(e) => {
                        let _ = ready_tx.send(Err(e));
                    }
                }
            })
            .map_err(|e| format!("audio thread: {e}"))?;

        ready_rx.recv().map_err(|_| "audio thread died".to_string())??;
        Ok(Audio { mic: mic_rx, speaker: spk_tx, _stop: stop_tx })
    }
}

/// How much we like a sample format (higher is better). 8-bit is a last resort.
fn format_rank(f: SampleFormat) -> u8 {
    match f {
        SampleFormat::F32 => 9,
        SampleFormat::I16 => 8,
        SampleFormat::I32 => 7,
        SampleFormat::F64 => 6,
        SampleFormat::U16 => 5,
        SampleFormat::U32 => 4,
        SampleFormat::I8 => 2,
        SampleFormat::U8 => 1,
        _ => 0,
    }
}

/// Prefer a config at exactly 16 kHz (PipeWire/Pulse resample for us) in the best sample format
/// the device offers; otherwise fall back to the device default.
fn pick_config(
    ranges: impl Iterator<Item = cpal::SupportedStreamConfigRange>,
    default: cpal::SupportedStreamConfig,
) -> cpal::SupportedStreamConfig {
    let best = ranges
        .filter(|r| r.min_sample_rate().0 <= RATE && RATE <= r.max_sample_rate().0)
        .max_by_key(|r| format_rank(r.sample_format()));
    match best {
        // Take it unless the default is clearly better than what 16 kHz offers.
        Some(r) if format_rank(r.sample_format()) >= format_rank(default.sample_format()) => {
            r.with_sample_rate(SampleRate(RATE))
        }
        _ => default,
    }
}

// ---------------------------------------------------------------- capture

fn build_input(device: &cpal::Device, tx: async_channel::Sender<Vec<i16>>) -> Result<Stream, String> {
    let default = device.default_input_config().map_err(|e| format!("microphone config: {e}"))?;
    let ranges = device.supported_input_configs().map_err(|e| format!("microphone configs: {e}"))?;
    let chosen = pick_config(ranges, default);
    let format = chosen.sample_format();
    let config: StreamConfig = chosen.into();
    match format {
        SampleFormat::F32 => input_stream::<f32>(device, &config, tx),
        SampleFormat::I16 => input_stream::<i16>(device, &config, tx),
        SampleFormat::U16 => input_stream::<u16>(device, &config, tx),
        SampleFormat::I32 => input_stream::<i32>(device, &config, tx),
        SampleFormat::U32 => input_stream::<u32>(device, &config, tx),
        SampleFormat::F64 => input_stream::<f64>(device, &config, tx),
        SampleFormat::I8 => input_stream::<i8>(device, &config, tx),
        SampleFormat::U8 => input_stream::<u8>(device, &config, tx),
        other => Err(format!("unsupported microphone sample format {other:?}")),
    }
}

fn input_stream<T>(device: &cpal::Device, config: &StreamConfig, tx: async_channel::Sender<Vec<i16>>) -> Result<Stream, String>
where
    T: cpal::SizedSample + Send + 'static,
    f32: cpal::FromSample<T>,
{
    let channels = config.channels as usize;
    let mut down = Downsampler::new(config.sample_rate.0);
    let mut pending: Vec<i16> = Vec::with_capacity(FRAME * 2);
    device
        .build_input_stream(
            config,
            move |data: &[T], _| {
                for frame in data.chunks(channels) {
                    // Average the channels into mono.
                    let mono = frame.iter().map(|s| f32::from_sample(*s)).sum::<f32>() / channels as f32;
                    if let Some(y) = down.push(mono) {
                        pending.push((y.clamp(-1.0, 1.0) * 32767.0) as i16);
                        if pending.len() == FRAME {
                            // Full queue: drop the frame rather than block the audio thread.
                            let _ = tx.try_send(std::mem::replace(&mut pending, Vec::with_capacity(FRAME * 2)));
                        }
                    }
                }
            },
            |e| eprintln!("audio: microphone error: {e}"),
            None,
        )
        .map_err(|e| format!("microphone: {e}"))
}

/// Box-filter decimation by a (possibly fractional) factor. Rate == 16 kHz is a pass-through.
struct Downsampler {
    step: f64,
    phase: f64,
    acc: f32,
    n: u32,
}
impl Downsampler {
    fn new(device_rate: u32) -> Self {
        Self { step: (device_rate as f64 / RATE as f64).max(1.0), phase: 0.0, acc: 0.0, n: 0 }
    }
    fn push(&mut self, x: f32) -> Option<f32> {
        self.acc += x;
        self.n += 1;
        self.phase += 1.0;
        if self.phase >= self.step {
            self.phase -= self.step;
            let y = self.acc / self.n as f32;
            self.acc = 0.0;
            self.n = 0;
            Some(y)
        } else {
            None
        }
    }
}

// ---------------------------------------------------------------- playout

fn build_output(device: &cpal::Device, rx: async_channel::Receiver<Vec<i16>>) -> Result<Stream, String> {
    let default = device.default_output_config().map_err(|e| format!("speaker config: {e}"))?;
    let ranges = device.supported_output_configs().map_err(|e| format!("speaker configs: {e}"))?;
    let chosen = pick_config(ranges, default);
    let format = chosen.sample_format();
    let config: StreamConfig = chosen.into();
    match format {
        SampleFormat::F32 => output_stream::<f32>(device, &config, rx),
        SampleFormat::I16 => output_stream::<i16>(device, &config, rx),
        SampleFormat::U16 => output_stream::<u16>(device, &config, rx),
        SampleFormat::I32 => output_stream::<i32>(device, &config, rx),
        SampleFormat::U32 => output_stream::<u32>(device, &config, rx),
        SampleFormat::F64 => output_stream::<f64>(device, &config, rx),
        SampleFormat::I8 => output_stream::<i8>(device, &config, rx),
        SampleFormat::U8 => output_stream::<u8>(device, &config, rx),
        other => Err(format!("unsupported speaker sample format {other:?}")),
    }
}

fn output_stream<T>(device: &cpal::Device, config: &StreamConfig, rx: async_channel::Receiver<Vec<i16>>) -> Result<Stream, String>
where
    T: cpal::SizedSample + cpal::FromSample<f32> + Send + 'static,
{
    let channels = config.channels as usize;
    // Source samples consumed per output sample (<= 1 when the device runs faster than 16 kHz).
    let step = RATE as f64 / config.sample_rate.0 as f64;
    let mut queue: VecDeque<f32> = VecDeque::new();
    let mut pos = 0.0f64;
    device
        .build_output_stream(
            config,
            move |out: &mut [T], _| {
                while let Ok(frame) = rx.try_recv() {
                    queue.extend(frame.iter().map(|s| *s as f32 / 32768.0));
                }
                if queue.len() > MAX_PLAYOUT_SAMPLES {
                    let drop_n = queue.len() - MAX_PLAYOUT_SAMPLES;
                    queue.drain(..drop_n);
                }
                for frame in out.chunks_mut(channels) {
                    let sample = if queue.len() >= 2 {
                        let (a, b) = (queue[0], queue[1]);
                        let y = a + (b - a) * pos as f32;
                        pos += step;
                        while pos >= 1.0 && !queue.is_empty() {
                            pos -= 1.0;
                            queue.pop_front();
                        }
                        y
                    } else {
                        0.0 // underrun: silence
                    };
                    let value = T::from_sample(sample);
                    for slot in frame.iter_mut() {
                        *slot = value;
                    }
                }
            },
            |e| eprintln!("audio: speaker error: {e}"),
            None,
        )
        .map_err(|e| format!("speaker: {e}"))
}
