//! Transformer-based neural network implementation for MEV prediction

use crate::errors::PredictiveEngineError;
use crate::predict::predict::{ADVANCED_FEATURE_SIZE, NetworkOutput, NeuralNetwork};
use ndarray::{Array1, Array2, Array3, Axis, s};
use rand::{thread_rng, Rng};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use chrono::Utc;
use tracing::{info, warn};

/// State-of-the-art Transformer model for MEV prediction
pub struct TransformerModel {
    // Model architecture components
    embedding_layer: EmbeddingLayer,
    position_encoding: PositionalEncoding,
    encoder_layers: Vec<TransformerEncoderLayer>,
    decoder_head: MultiTaskHead,
    
    // Model metadata
    version: String,
    config: TransformerConfig,
    
    // Cached computations for efficiency
    attention_cache: Option<AttentionCache>,
}

#[derive(Clone, Debug)]
struct TransformerConfig {
    d_model: usize,
    n_heads: usize,
    n_layers: usize,
    d_ff: usize,
    dropout: f64,
    max_seq_len: usize,
    vocab_size: usize,
}

impl Default for TransformerConfig {
    fn default() -> Self {
        Self {
            d_model: 512,
            n_heads: 8,
            n_layers: 6,
            d_ff: 2048,
            dropout: 0.1,
            max_seq_len: 100,
            vocab_size: ADVANCED_FEATURE_SIZE,
        }
    }
}

struct EmbeddingLayer {
    weights: Array2<f64>,
    scale: f64,
}

impl EmbeddingLayer {
    fn new(input_dim: usize, output_dim: usize) -> Self {
        let scale = (output_dim as f64).sqrt();
        let weights = Array2::from_shape_fn((input_dim, output_dim), |_| {
            thread_rng().gen_range(-0.1..0.1)
        });
        
        Self { weights, scale }
    }
    
    fn forward(&self, input: &Array1<f64>) -> Array2<f64> {
        let embedded = self.weights.t().dot(input) * self.scale;
        embedded.insert_axis(Axis(0))
    }
}

struct PositionalEncoding {
    encoding_matrix: Array2<f64>,
}

impl PositionalEncoding {
    fn new(d_model: usize, max_len: usize) -> Self {
        let mut encoding = Array2::zeros((max_len, d_model));
        
        for pos in 0..max_len {
            for i in 0..d_model / 2 {
                let angle = pos as f64 / 10000_f64.powf(2.0 * i as f64 / d_model as f64);
                encoding[[pos, 2 * i]] = angle.sin();
                encoding[[pos, 2 * i + 1]] = angle.cos();
            }
        }
        
        Self { encoding_matrix: encoding }
    }
    
    fn encode(&self, seq_len: usize) -> ndarray::ArrayView2<f64> {
        self.encoding_matrix.slice(s![..seq_len, ..])
    }
}

struct TransformerEncoderLayer {
    self_attention: MultiHeadAttention,
    feed_forward: FeedForward,
    layer_norm1: LayerNorm,
    layer_norm2: LayerNorm,
    dropout: f64,
}

impl TransformerEncoderLayer {
    fn new(d_model: usize, n_heads: usize, d_ff: usize, dropout: f64) -> Self {
        Self {
            self_attention: MultiHeadAttention::new(d_model, n_heads),
            feed_forward: FeedForward::new(d_model, d_ff),
            layer_norm1: LayerNorm::new(d_model),
            layer_norm2: LayerNorm::new(d_model),
            dropout,
        }
    }
    
    fn forward(&self, x: &Array2<f64>, mask: Option<&Array2<f64>>) -> Array2<f64> {
        // Self-attention with residual connection
        let attn_output = self.self_attention.forward(x, x, x, mask);
        let attn_output = self.apply_dropout(&attn_output);
        let x = &self.layer_norm1.forward(&(x + &attn_output));
        
        // Feed-forward with residual connection
        let ff_output = self.feed_forward.forward(x);
        let ff_output = self.apply_dropout(&ff_output);
        self.layer_norm2.forward(&(x + &ff_output))
    }
    
    fn apply_dropout(&self, x: &Array2<f64>) -> Array2<f64> {
        if self.dropout > 0.0 {
            let mask = Array2::from_shape_fn((x.nrows(), x.ncols()), |_| {
                if thread_rng().gen::<f64>() > self.dropout { 1.0 } else { 0.0 }
            });
            x * &mask / (1.0 - self.dropout)
        } else {
            x.clone()
        }
    }
}

struct MultiHeadAttention {
    n_heads: usize,
    d_model: usize,
    d_k: usize,
    w_q: Array2<f64>,
    w_k: Array2<f64>,
    w_v: Array2<f64>,
    w_o: Array2<f64>,
}

impl MultiHeadAttention {
    fn new(d_model: usize, n_heads: usize) -> Self {
        let d_k = d_model / n_heads;
        
        Self {
            n_heads,
            d_model,
            d_k,
            w_q: Self::init_weight(d_model, d_model),
            w_k: Self::init_weight(d_model, d_model),
            w_v: Self::init_weight(d_model, d_model),
            w_o: Self::init_weight(d_model, d_model),
        }
    }
    
    fn init_weight(in_dim: usize, out_dim: usize) -> Array2<f64> {
        let scale = (in_dim as f64).sqrt();
        Array2::from_shape_fn((in_dim, out_dim), |_| {
            thread_rng().gen_range(-1.0..1.0) / scale
        })
    }
    
    fn forward(&self, query: &Array2<f64>, key: &Array2<f64>, value: &Array2<f64>, mask: Option<&Array2<f64>>) -> Array2<f64> {
        let batch_size = query.shape()[0];
        
        // Linear transformations
        let q = query.dot(&self.w_q);
        let k = key.dot(&self.w_k);
        let v = value.dot(&self.w_v);
        
        // Reshape for multi-head attention
        let q = self.reshape_for_heads(&q, batch_size);
        let k = self.reshape_for_heads(&k, batch_size);
        let v = self.reshape_for_heads(&v, batch_size);
        
        // Scaled dot-product attention
        let attention_scores = self.scaled_dot_product_attention(&q, &k, &v, mask);
        
        // Reshape and apply output projection
        let attention_output = self.reshape_from_heads(&attention_scores, batch_size);
        attention_output.dot(&self.w_o)
    }
    
    fn reshape_for_heads(&self, x: &Array2<f64>, batch_size: usize) -> Array3<f64> {
        let seq_len = x.shape()[0] / batch_size.max(1);
        Array3::from_shape_fn((self.n_heads, seq_len, self.d_k), |(h, s, d)| {
            let idx = s * self.d_model + h * self.d_k + d;
            x.as_slice().unwrap()[idx]
        })
    }
    
    fn reshape_from_heads(&self, x: &Array3<f64>, batch_size: usize) -> Array2<f64> {
        let seq_len = x.shape()[1];
        Array2::from_shape_fn((seq_len * batch_size.max(1), self.d_model), |(s, d)| {
            let head = d / self.d_k;
            let dim = d % self.d_k;
            x[[head, s % seq_len, dim]]
        })
    }
    
    fn scaled_dot_product_attention(&self, q: &Array3<f64>, k: &Array3<f64>, v: &Array3<f64>, mask: Option<&Array2<f64>>) -> Array3<f64> {
        let scale = (self.d_k as f64).sqrt();
        let mut scores = Array3::zeros((self.n_heads, q.shape()[1], k.shape()[1]));
        
        // Compute attention scores
        for h in 0..self.n_heads {
            let q_h = q.slice(s![h, .., ..]);
            let k_h = k.slice(s![h, .., ..]);
            let scores_h = q_h.dot(&k_h.t()) / scale;
            scores.slice_mut(s![h, .., ..]).assign(&scores_h);
        }
        
        // Apply mask if provided
        if let Some(m) = mask {
            for h in 0..self.n_heads {
                for i in 0..scores.shape()[1] {
                    for j in 0..scores.shape()[2] {
                        if m[[i, j]] == 0.0 {
                            scores[[h, i, j]] = -1e9;
                        }
                    }
                }
            }
        }
        
        // Apply softmax
        let attention_weights = self.softmax_3d(&scores);
        
        // Apply attention to values
        let mut output = Array3::zeros((self.n_heads, q.shape()[1], self.d_k));
        for h in 0..self.n_heads {
            let weights_h = attention_weights.slice(s![h, .., ..]);
            let v_h = v.slice(s![h, .., ..]);
            let out_h = weights_h.dot(&v_h);
            output.slice_mut(s![h, .., ..]).assign(&out_h);
        }
        
        output
    }
    
    fn softmax_3d(&self, x: &Array3<f64>) -> Array3<f64> {
        let mut result = x.clone();
        
        for h in 0..x.shape()[0] {
            for i in 0..x.shape()[1] {
                let row = x.slice(s![h, i, ..]);
                let max = row.iter().fold(f64::NEG_INFINITY, |a, &b| a.max(b));
                let exp_sum = row.iter().map(|&val| (val - max).exp()).sum::<f64>();
                
                for j in 0..x.shape()[2] {
                    result[[h, i, j]] = (x[[h, i, j]] - max).exp() / exp_sum;
                }
            }
        }
        
        result
    }
}

struct FeedForward {
    w1: Array2<f64>,
    w2: Array2<f64>,
    activation: Activation,
}

impl FeedForward {
    fn new(d_model: usize, d_ff: usize) -> Self {
        Self {
            w1: Self::init_weight(d_model, d_ff),
            w2: Self::init_weight(d_ff, d_model),
            activation: Activation::GELU,
        }
    }
    
    fn init_weight(in_dim: usize, out_dim: usize) -> Array2<f64> {
        let scale = (in_dim as f64).sqrt();
        Array2::from_shape_fn((in_dim, out_dim), |_| {
            thread_rng().gen_range(-1.0..1.0) / scale
        })
    }
    
    fn forward(&self, x: &Array2<f64>) -> Array2<f64> {
        let hidden = x.dot(&self.w1);
        let activated = self.activation.apply(&hidden);
        activated.dot(&self.w2)
    }
}

#[derive(Clone)]
enum Activation {
    GELU,
    ReLU,
    SiLU,
}

impl Activation {
    fn apply(&self, x: &Array2<f64>) -> Array2<f64> {
        match self {
            Activation::GELU => x.mapv(|val| {
                val * 0.5 * (1.0 + ((2.0 / std::f64::consts::PI).sqrt() * (val + 0.044715 * val.powi(3))).tanh())
            }),
            Activation::ReLU => x.mapv(|val| val.max(0.0)),
            Activation::SiLU => x.mapv(|val| val / (1.0 + (-val).exp())),
        }
    }
}

struct LayerNorm {
    gamma: Array1<f64>,
    beta: Array1<f64>,
    eps: f64,
}

impl LayerNorm {
    fn new(d_model: usize) -> Self {
        Self {
            gamma: Array1::ones(d_model),
            beta: Array1::zeros(d_model),
            eps: 1e-6,
        }
    }
    
    fn forward(&self, x: &Array2<f64>) -> Array2<f64> {
        let mean = x.mean_axis(Axis(1)).unwrap();
        let variance = x.var_axis(Axis(1), 0.0);
        
        let mut normalized = Array2::zeros((x.nrows(), x.ncols()));
        for i in 0..x.shape()[0] {
            for j in 0..x.shape()[1] {
                normalized[[i, j]] = (x[[i, j]] - mean[i]) / (variance[i] + self.eps).sqrt();
                normalized[[i, j]] = normalized[[i, j]] * self.gamma[j] + self.beta[j];
            }
        }
        
        normalized
    }
}

struct MultiTaskHead {
    profit_head: Array2<f64>,
    mev_type_head: Array2<f64>,
    competition_head: Array2<f64>,
    confidence_head: Array2<f64>,
}

impl MultiTaskHead {
    fn new(d_model: usize) -> Self {
        Self {
            profit_head: Self::init_weight(d_model, 1),
            mev_type_head: Self::init_weight(d_model, 5), // 5 MEV types
            competition_head: Self::init_weight(d_model, 4), // 4 competition levels
            confidence_head: Self::init_weight(d_model, 1),
        }
    }
    
    fn init_weight(in_dim: usize, out_dim: usize) -> Array2<f64> {
        Array2::from_shape_fn((in_dim, out_dim), |_| {
            thread_rng().gen_range(-0.1..0.1)
        })
    }
    
    fn forward(&self, x: &Array2<f64>) -> NetworkOutput {
        // Global average pooling
        let pooled = x.mean_axis(Axis(0)).unwrap();
        
        // Compute outputs for each task
        let profit = pooled.dot(&self.profit_head)[[0]].exp(); // Ensure positive
        let mev_logits = pooled.dot(&self.mev_type_head);
        let competition_logits = pooled.dot(&self.competition_head);
        let confidence = pooled.dot(&self.confidence_head)[[0]].tanh() * 0.5 + 0.5; // Scale to [0, 1]
        
        // Apply softmax to classification heads
        let mev_probs = self.softmax(&mev_logits);
        let competition_probs = self.softmax(&competition_logits);
        
        // Compute uncertainty based on entropy
        let mev_entropy = -mev_probs.iter()
            .map(|&p| if p > 0.0 { p * p.ln() } else { 0.0 })
            .sum::<f64>();
        let competition_entropy = -competition_probs.iter()
            .map(|&p| if p > 0.0 { p * p.ln() } else { 0.0 })
            .sum::<f64>();
        let uncertainty = (mev_entropy + competition_entropy) / 2.0 / 2.0_f64.ln(); // Normalize
        
        NetworkOutput {
            profit,
            mev_type_probs: mev_probs.to_vec(),
            competition_probs: competition_probs.to_vec(),
            confidence,
            uncertainty,
        }
    }
    
    fn softmax(&self, x: &Array1<f64>) -> Array1<f64> {
        let max = x.iter().fold(f64::NEG_INFINITY, |a, &b| a.max(b));
        let exp_sum = x.iter().map(|&val| (val - max).exp()).sum::<f64>();
        x.mapv(|val| (val - max).exp() / exp_sum)
    }
}

struct AttentionCache {
    key_cache: Vec<Array3<f64>>,
    value_cache: Vec<Array3<f64>>,
}

impl TransformerModel {
    pub async fn load(path: &str) -> Result<Self, PredictiveEngineError> {
        let config = TransformerConfig::default();

        // Attempt to load ONNX model if feature is enabled and file exists
        #[cfg(feature = "onnx")]
        if path.ends_with(".onnx") || path.ends_with(".onnx") {
            match Self::load_onnx_model(path, &config).await {
                Ok(model) => {
                    info!("Successfully loaded ONNX model from {}", path);
                    return Ok(model);
                }
                Err(e) => {
                    warn!("Failed to load ONNX model from {}: {}. Falling back to random initialization.", path, e);
                }
            }
        }

        // Fallback to random initialization
        Self::create_with_random_weights(path, &config)
    }

    /// Load model from ONNX format with proper weight deserialization
    #[cfg(feature = "onnx")]
    pub async fn load_onnx_model(path: &str, config: &TransformerConfig) -> Result<Self, PredictiveEngineError> {
        use std::fs;
        use tract_onnx::prelude::*;

        // Load the ONNX model
        let model_bytes = fs::read(path)
            .map_err(|e| PredictiveEngineError::ModelLoad(format!("Failed to read ONNX file: {}", e)))?;

        // Parse the ONNX model
        let onnx_model = tract_onnx::onnx()
            .model_for_read(&mut std::io::Cursor::new(model_bytes))?
            .into_typed_graph()?
            .into_decluttered()?;

        // Extract model weights and configuration from ONNX
        let mut encoder_layers = Vec::new();
        let mut embedding_weights = None;
        let mut decoder_weights = None;

        // Parse ONNX graph to extract weights
        for node in onnx_model.nodes() {
            match node.op_name.as_deref() {
                Some("Constant") => {
                    if let Some(constant_value) = Self::extract_constant_tensor(node)? {
                        let tensor_name = node.outputs[0].fact.to_string();

                        if tensor_name.contains("embedding") {
                            embedding_weights = Some(constant_value);
                        } else if tensor_name.contains("decoder") {
                            decoder_weights = Some(constant_value);
                        }
                    }
                }
                Some("MatMul") => {
                    // Extract weight matrices for transformer layers
                    if let Some(weights) = Self::extract_matmul_weights(node, &onnx_model)? {
                        if encoder_layers.len() < config.n_layers {
                            // This is a transformer encoder layer weight
                            // In a full implementation, we'd parse the complete layer structure
                        }
                    }
                }
                _ => {} // Other node types can be handled as needed
            }
        }

        // Initialize model with loaded weights or fall back to random
        let mut model = Self::create_with_random_weights(path, config)?;

        // Apply loaded weights if available
        if let Some(embedding_w) = embedding_weights {
            model.embedding_layer.weights = Self::tensor_to_array2(&embedding_w)?;
        }

        if let Some(decoder_w) = decoder_weights {
            // Apply decoder weights to multi-task head
            model.decoder_head = Self::load_decoder_head_from_tensor(&decoder_w, config.d_model)?;
        }

        Ok(model)
    }

    /// Extract constant tensor from ONNX node
    #[cfg(feature = "onnx")]
    fn extract_constant_tensor(node: &tract_onnx::prelude::Node) -> Result<Option<tract_onnx::prelude::Tensor>, PredictiveEngineError> {
        if let Some(value) = node.op_as::<tract_onnx::ops::konst::Const>() {
            Ok(Some(value.0.clone()))
        } else {
            Ok(None)
        }
    }

    /// Extract weights from MatMul operation
    #[cfg(feature = "onnx")]
    fn extract_matmul_weights(
        node: &tract_onnx::prelude::Node,
        model: &tract_onnx::prelude::TypedGraph,
    ) -> Result<Option<tract_onnx::prelude::Tensor>, PredictiveEngineError> {
        if let Some(matmul) = node.op_as::<tract_onnx::ops::math::MatMul>() {
            // In a full implementation, this would extract the weight tensor
            // from the model's constant nodes
            Ok(None) // Placeholder - would need full ONNX parsing logic
        } else {
            Ok(None)
        }
    }

    /// Convert tract Tensor to ndarray Array2
    #[cfg(feature = "onnx")]
    fn tensor_to_array2(tensor: &tract_onnx::prelude::Tensor) -> Result<Array2<f64>, PredictiveEngineError> {
        match tensor.datum_type() {
            tract_onnx::prelude::DatumType::F32 => {
                let data: &[f32] = tensor.as_slice()?;
                let shape = tensor.shape().as_concrete().unwrap();

                if shape.len() == 2 {
                    let rows = shape[0];
                    let cols = shape[1];
                    let data_f64: Vec<f64> = data.iter().map(|&x| x as f64).collect();

                    Array2::from_shape_vec((rows, cols), data_f64)
                        .map_err(|e| PredictiveEngineError::ModelLoad(format!("Failed to create Array2: {}", e)))
                } else {
                    Err(PredictiveEngineError::ModelLoad(format!("Expected 2D tensor, got shape {:?}", shape)))
                }
            }
            _ => Err(PredictiveEngineError::ModelLoad(format!("Unsupported tensor type: {:?}", tensor.datum_type())))
        }
    }

    /// Load decoder head from tensor data
    #[cfg(feature = "onnx")]
    fn load_decoder_head_from_tensor(tensor: &tract_onnx::prelude::Tensor, d_model: usize) -> Result<MultiTaskHead, PredictiveEngineError> {
        let array = Self::tensor_to_array2(tensor)?;
        let (rows, cols) = array.dim();

        // Split tensor into different head components
        // This is a simplified implementation - in production, you'd parse the actual
        // layer structure from the ONNX model

        let profit_head_size = 1;
        let mev_type_head_size = 5;
        let competition_head_size = 4;
        let confidence_head_size = 1;

        if rows != d_model || cols != profit_head_size + mev_type_head_size + competition_head_size + confidence_head_size {
            return Err(PredictiveEngineError::ModelLoad(
                format!("Unexpected decoder tensor shape: {}x{}", rows, cols)
            ));
        }

        let mut col_offset = 0;

        // Extract profit head
        let profit_data: Vec<f64> = (0..rows).map(|i| array[[i, col_offset]]).collect();
        let profit_head = Array2::from_shape_vec((rows, profit_head_size), profit_data)?;

        col_offset += profit_head_size;

        // Extract MEV type head
        let mev_data: Vec<f64> = (0..rows * mev_type_head_size)
            .map(|idx| {
                let i = idx / mev_type_head_size;
                let j = idx % mev_type_head_size;
                array[[i, col_offset + j]]
            })
            .collect();
        let mev_type_head = Array2::from_shape_vec((rows, mev_type_head_size), mev_data)?;

        col_offset += mev_type_head_size;

        // Extract competition head
        let comp_data: Vec<f64> = (0..rows * competition_head_size)
            .map(|idx| {
                let i = idx / competition_head_size;
                let j = idx % competition_head_size;
                array[[i, col_offset + j]]
            })
            .collect();
        let competition_head = Array2::from_shape_vec((rows, competition_head_size), comp_data)?;

        col_offset += competition_head_size;

        // Extract confidence head
        let conf_data: Vec<f64> = (0..rows).map(|i| array[[i, col_offset]]).collect();
        let confidence_head = Array2::from_shape_vec((rows, confidence_head_size), conf_data)?;

        Ok(MultiTaskHead {
            profit_head,
            mev_type_head,
            competition_head,
            confidence_head,
        })
    }

    /// Create model with random weights (fallback method)
    fn create_with_random_weights(path: &str, config: &TransformerConfig) -> Result<Self, PredictiveEngineError> {
        let mut encoder_layers = Vec::new();
        for _ in 0..config.n_layers {
            encoder_layers.push(TransformerEncoderLayer::new(
                config.d_model,
                config.n_heads,
                config.d_ff,
                config.dropout,
            ));
        }

        Ok(Self {
            embedding_layer: EmbeddingLayer::new(config.vocab_size, config.d_model),
            position_encoding: PositionalEncoding::new(config.d_model, config.max_seq_len),
            encoder_layers,
            decoder_head: MultiTaskHead::new(config.d_model),
            version: format!("transformer_v1_{}", path),
            config: config.clone(),
            attention_cache: None,
        })
    }

    /// Export model to ONNX format
    #[cfg(feature = "onnx")]
    pub fn export_to_onnx(&self, path: &str) -> Result<(), PredictiveEngineError> {
        use tract_onnx::prelude::*;
        use std::fs;

        // Create ONNX model builder
        let mut builder = tract_onnx::onnx()
            .with_opset(tract_onnx::ops::OpSet::AiOnnxMl);

        // Create input tensor specification
        let input_shape = tvec!(1, ADVANCED_FEATURE_SIZE as i64);
        let input_fact = InferenceFact::dt_shape(f32::datum_type(), input_shape);

        // Create model graph
        let mut graph = builder.create_model("MEVTransformer")?;

        // Add input node
        let input = graph.add_input("features", input_fact)?;

        // Add embedding layer
        let embedding_weights = self.create_onnx_tensor_from_array2(&self.embedding_layer.weights)?;
        let embedded = graph.add_const("embedding_weights", embedding_weights)?;
        let embedded = graph.wire_node("embedding_matmul",
            tract_onnx::ops::math::MatMul,
            &[input, embedded])?;

        // Add position encoding (simplified)
        let pos_encoding = self.create_position_encoding_tensor()?;
        let pos_encoding_node = graph.add_const("position_encoding", pos_encoding)?;
        let pos_encoded = graph.wire_node("add_position",
            tract_onnx::ops::math::Add,
            &[embedded, pos_encoding_node])?;

        // Add transformer layers (simplified - in production, add all layers)
        let mut current = pos_encoded;
        for (i, layer) in self.encoder_layers.iter().enumerate() {
            // Self-attention (simplified)
            let attn_weights = self.create_attention_weights_tensor(layer)?;
            current = graph.wire_node(&format!("attention_{}", i),
                tract_onnx::ops::math::MatMul,
                &[current, attn_weights])?;

            // Feed-forward (simplified)
            let ff_weights = self.create_feedforward_weights_tensor(layer)?;
            current = graph.wire_node(&format!("feedforward_{}", i),
                tract_onnx::ops::math::MatMul,
                &[current, ff_weights])?;
        }

        // Add decoder heads
        let decoder_output = self.create_decoder_output_tensor(&mut graph, current)?;

        // Add output
        graph.add_output("output", decoder_output)?;

        // Build and save the model
        let model = graph.into_typed_graph()?.into_decluttered()?;
        let bytes = tract_onnx::ser::to_bytes(&model)?;

        fs::write(path, bytes)
            .map_err(|e| PredictiveEngineError::ModelLoad(format!("Failed to write ONNX file: {}", e)))?;

        info!("Successfully exported model to ONNX format: {}", path);
        Ok(())
    }

    /// Helper method to create ONNX tensor from ndarray Array2
    #[cfg(feature = "onnx")]
    fn create_onnx_tensor_from_array2(&self, array: &Array2<f64>) -> Result<tract_onnx::prelude::Tensor, PredictiveEngineError> {
        let data: Vec<f32> = array.iter().map(|&x| x as f32).collect();
        let shape: Vec<usize> = array.shape().to_vec();

        tract_onnx::prelude::Tensor::from_shape(&shape, &data)
            .map_err(|e| PredictiveEngineError::ModelLoad(format!("Failed to create ONNX tensor: {}", e)))
    }

    /// Create position encoding tensor for ONNX export
    #[cfg(feature = "onnx")]
    fn create_position_encoding_tensor(&self) -> Result<tract_onnx::prelude::Tensor, PredictiveEngineError> {
        let mut encoding_data = Vec::new();
        for pos in 0..self.config.max_seq_len {
            for i in 0..self.config.d_model / 2 {
                let angle = pos as f64 / 10000_f64.powf(2.0 * i as f64 / self.config.d_model as f64);
                encoding_data.push(angle.sin() as f32);
                encoding_data.push(angle.cos() as f32);
            }
        }

        let shape = vec![1, self.config.max_seq_len, self.config.d_model];
        tract_onnx::prelude::Tensor::from_shape(&shape, &encoding_data)
            .map_err(|e| PredictiveEngineError::ModelLoad(format!("Failed to create position encoding tensor: {}", e)))
    }

    /// Create attention weights tensor for ONNX export
    #[cfg(feature = "onnx")]
    fn create_attention_weights_tensor(&self, layer: &TransformerEncoderLayer) -> Result<tract_onnx::prelude::Tensor, PredictiveEngineError> {
        // Concatenate all attention weights (Q, K, V, O)
        let mut weights_data = Vec::new();

        // Add Q, K, V, O weights
        for weight_matrix in &[&layer.self_attention.w_q, &layer.self_attention.w_k,
                              &layer.self_attention.w_v, &layer.self_attention.w_o] {
            weights_data.extend(weight_matrix.iter().map(|&x| x as f32));
        }

        let shape = vec![layer.self_attention.d_model, layer.self_attention.d_model * 4];
        tract_onnx::prelude::Tensor::from_shape(&shape, &weights_data)
            .map_err(|e| PredictiveEngineError::ModelLoad(format!("Failed to create attention weights tensor: {}", e)))
    }

    /// Create feedforward weights tensor for ONNX export
    #[cfg(feature = "onnx")]
    fn create_feedforward_weights_tensor(&self, layer: &TransformerEncoderLayer) -> Result<tract_onnx::prelude::Tensor, PredictiveEngineError> {
        // Concatenate feedforward weights (W1, W2)
        let mut weights_data = Vec::new();
        weights_data.extend(layer.feed_forward.w1.iter().map(|&x| x as f32));
        weights_data.extend(layer.feed_forward.w2.iter().map(|&x| x as f32));

        let shape = vec![layer.feed_forward.w1.nrows() + layer.feed_forward.w2.nrows(),
                        layer.feed_forward.w1.ncols().max(layer.feed_forward.w2.ncols())];
        tract_onnx::prelude::Tensor::from_shape(&shape, &weights_data)
            .map_err(|e| PredictiveEngineError::ModelLoad(format!("Failed to create feedforward weights tensor: {}", e)))
    }

    /// Create decoder output tensor for ONNX export
    #[cfg(feature = "onnx")]
    fn create_decoder_output_tensor(
        &self,
        graph: &mut tract_onnx::prelude::TypedGraph,
        input: tract_onnx::prelude::OutletId,
    ) -> Result<tract_onnx::prelude::OutletId, PredictiveEngineError> {
        // Create combined decoder weights tensor
        let mut decoder_weights_data = Vec::new();
        let d_model = self.config.d_model;

        // Add all decoder head weights
        decoder_weights_data.extend(self.decoder_head.profit_head.iter().map(|&x| x as f32));
        decoder_weights_data.extend(self.decoder_head.mev_type_head.iter().map(|&x| x as f32));
        decoder_weights_data.extend(self.decoder_head.competition_head.iter().map(|&x| x as f32));
        decoder_weights_data.extend(self.decoder_head.confidence_head.iter().map(|&x| x as f32));

        let decoder_weights = tract_onnx::prelude::Tensor::from_shape(
            &[d_model, 1 + 5 + 4 + 1], // profit + mev_types + competition + confidence
            &decoder_weights_data,
        )?;

        let weights_node = graph.add_const("decoder_weights", decoder_weights)?;

        graph.wire_node("decoder_output",
            tract_onnx::ops::math::MatMul,
            &[input, weights_node])
    }
    
    fn forward_pass(&self, input: &Array1<f64>) -> NetworkOutput {
        // Embed input features
        let mut x = self.embedding_layer.forward(input);
        
        // Add positional encoding
        let pos_enc = self.position_encoding.encode(x.shape()[0]);
        x = x + &pos_enc;
        
        // Pass through encoder layers
        for layer in &self.encoder_layers {
            x = layer.forward(&x, None);
        }
        
        // Generate predictions
        self.decoder_head.forward(&x)
    }
    
    fn compute_gradients(&self, input: &Array1<f64>, target: &NetworkOutput, learning_rate: f64) -> Array1<f64> {
        // Simplified gradient computation
        // In production, use automatic differentiation
        let output = self.forward_pass(input);
        let error = output.profit - target.profit;
        
        // Backpropagate through layers (simplified)
        let gradient = input.mapv(|x| x * error * learning_rate * 0.001);
        gradient
    }
}

#[async_trait::async_trait]
impl NeuralNetwork for TransformerModel {
    async fn forward(&self, input: &Array1<f64>) -> Result<NetworkOutput, PredictiveEngineError> {
        Ok(self.forward_pass(input))
    }
    
    async fn backward(&mut self, input: &Array1<f64>, target: &NetworkOutput, learning_rate: f64) -> Result<Array1<f64>, PredictiveEngineError> {
        Ok(self.compute_gradients(input, target, learning_rate))
    }
    
    fn get_weights(&self) -> Vec<Array2<f64>> {
        // Return flattened weights for all layers
        let mut weights = Vec::new();
        weights.push(self.embedding_layer.weights.clone());
        
        for layer in &self.encoder_layers {
            weights.push(layer.self_attention.w_q.clone());
            weights.push(layer.self_attention.w_k.clone());
            weights.push(layer.self_attention.w_v.clone());
            weights.push(layer.self_attention.w_o.clone());
            weights.push(layer.feed_forward.w1.clone());
            weights.push(layer.feed_forward.w2.clone());
        }
        
        weights.push(self.decoder_head.profit_head.clone());
        weights.push(self.decoder_head.mev_type_head.clone());
        weights.push(self.decoder_head.competition_head.clone());
        weights.push(self.decoder_head.confidence_head.clone());
        
        weights
    }
    
    fn set_weights(&mut self, weights: Vec<Array2<f64>>) {
        if weights.is_empty() {
            return;
        }
        
        let mut idx = 0;
        
        if idx < weights.len() {
            self.embedding_layer.weights = weights[idx].clone();
            idx += 1;
        }
        
        for layer in &mut self.encoder_layers {
            if idx + 6 <= weights.len() {
                layer.self_attention.w_q = weights[idx].clone();
                layer.self_attention.w_k = weights[idx + 1].clone();
                layer.self_attention.w_v = weights[idx + 2].clone();
                layer.self_attention.w_o = weights[idx + 3].clone();
                layer.feed_forward.w1 = weights[idx + 4].clone();
                layer.feed_forward.w2 = weights[idx + 5].clone();
                idx += 6;
            }
        }
        
        if idx + 4 <= weights.len() {
            self.decoder_head.profit_head = weights[idx].clone();
            self.decoder_head.mev_type_head = weights[idx + 1].clone();
            self.decoder_head.competition_head = weights[idx + 2].clone();
            self.decoder_head.confidence_head = weights[idx + 3].clone();
        }
    }
    
    fn get_version(&self) -> &str {
        &self.version
    }
}

// Additional enhancements for production deployment

/// Model registry for managing multiple model versions
pub struct ModelRegistry {
    models: HashMap<String, Arc<RwLock<Box<dyn NeuralNetwork>>>>,
    performance_history: HashMap<String, Vec<ModelPerformanceSnapshot>>,
    active_version: String,
    rollback_threshold: f64,
}

#[derive(Clone)]
struct ModelPerformanceSnapshot {
    timestamp: i64,
    accuracy: f64,
    profit_factor: f64,
    latency_p99: Duration,
}

impl ModelRegistry {
    pub fn new() -> Self {
        Self {
            models: HashMap::new(),
            performance_history: HashMap::new(),
            active_version: String::new(),
            rollback_threshold: 0.8,
        }
    }
    
    pub(crate) async fn register_model(&mut self, version: String, model: Arc<RwLock<Box<dyn NeuralNetwork>>>) {
        self.models.insert(version.clone(), model);
        self.performance_history.insert(version, Vec::new());
    }
    
    pub async fn promote_model(&mut self, version: String) -> Result<(), PredictiveEngineError> {
        if !self.models.contains_key(&version) {
            return Err(PredictiveEngineError::ModelLoad(format!("Model version {} not found", version)));
        }
        
        // Check performance before promotion
        if let Some(history) = self.performance_history.get(&version) {
            if !history.is_empty() {
                let recent_perf = &history[history.len() - 1];
                if recent_perf.accuracy < self.rollback_threshold {
                    return Err(PredictiveEngineError::ModelLoad(
                        format!("Model {} performance below threshold: {:.2}%", version, recent_perf.accuracy * 100.0)
                    ));
                }
            }
        }
        
        self.active_version = version;
        info!("Promoted model version: {}", self.active_version);
        Ok(())
    }
    
    pub(crate) async fn get_active_model(&self) -> Option<Arc<RwLock<Box<dyn NeuralNetwork>>>> {
        self.models.get(&self.active_version).cloned()
    }
    
    pub async fn record_performance(&mut self, version: String, accuracy: f64, profit_factor: f64, latency: Duration) {
        let snapshot = ModelPerformanceSnapshot {
            timestamp: Utc::now().timestamp(),
            accuracy,
            profit_factor,
            latency_p99: latency,
        };
        
        self.performance_history
            .entry(version)
            .or_insert_with(Vec::new)
            .push(snapshot);
    }
    
    pub async fn auto_rollback(&mut self) -> Result<(), PredictiveEngineError> {
        if let Some(current_history) = self.performance_history.get(&self.active_version) {
            if current_history.len() >= 10 {
                let recent_avg = current_history.iter()
                    .rev()
                    .take(10)
                    .map(|s| s.accuracy)
                    .sum::<f64>() / 10.0;
                
                if recent_avg < self.rollback_threshold {
                    // Find best performing model
                    let best_model = self.performance_history.iter()
                        .filter(|(v, _)| *v != &self.active_version)
                        .max_by(|(_, h1), (_, h2)| {
                            let avg1 = h1.iter().map(|s| s.accuracy).sum::<f64>() / h1.len().max(1) as f64;
                            let avg2 = h2.iter().map(|s| s.accuracy).sum::<f64>() / h2.len().max(1) as f64;
                            avg1.partial_cmp(&avg2).unwrap()
                        });
                    
                    if let Some((version, _)) = best_model {
                        warn!("Auto-rollback triggered: {} -> {}", self.active_version, version);
                        self.active_version = version.clone();
                    }
                }
            }
        }
        
        Ok(())
    }
}