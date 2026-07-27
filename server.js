'use strict';
/**
 * UROBOROS v4 — supercomputador de N clusters (Claude × Gemini)
 * Node 20+, zero dependências npm.
 *
 * v3: perfis de domínio, camada de fontes plugável, roteamento por confiança, árbitro.
 * v4: verificador determinístico, controle de acesso, upload de corpus,
 *     endurecimento HTTP e desligamento gracioso — pronto para hospedagem real.
 */

const http = require('http');
const https = require('https');
const fs = require('fs');
const fsp = require('fs/promises');
const path = require('path');
const crypto = require('crypto');

/* ------------------------------------------------------------------ config */
const CFG = {
  port: Number(process.env.PORT || 8080),
  anthropicKey: process.env.ANTHROPIC_API_KEY || '',
  geminiKey: process.env.GEMINI_API_KEY || '',
  claudeModel: process.env.CLAUDE_MODEL || 'claude-haiku-4-5-20251001',
  geminiModel: process.env.GEMINI_MODEL || 'gemini-2.5-flash',
  arbiterModel: process.env.ARBITER_MODEL || '',      // vazio = mesmo do CLAUDE_MODEL
  clusters: Number(process.env.CLUSTERS || 10),
  concurrency: Number(process.env.CONCURRENCY || 4),
  maxInput: Number(process.env.MAX_INPUT || 6000),
  maxTokens: Number(process.env.MAX_TOKENS || 1200),
  maxRounds: Number(process.env.MAX_ROUNDS || 3),
  convergeAt: Number(process.env.CONVERGE_AT || 0.12),
  confLimiar: Number(process.env.CONF_LIMIAR || 0.75), // acima disso o cluster é dispensado do refino
  divLimiar: Number(process.env.DIV_LIMIAR || 0.18),   // similaridade abaixo disso = divergência candidata
  retries: Number(process.env.RETRIES || 3),
  httpSource: process.env.SOURCE_HTTP || '',           // ex.: https://api.exemplo.com/busca?q={q}
  corpusDir: process.env.CORPUS_DIR || path.join(__dirname, 'corpus'),
  jobsDir: process.env.JOBS_DIR || path.join(__dirname, 'jobs'),
  keepJobs: Number(process.env.KEEP_JOBS || 60),
  accessToken: process.env.ACCESS_TOKEN || '',         // vazio = aberto (só faça isso em rede local)
  maxCostUSD: Number(process.env.MAX_COST_USD || 1.00), // teto por execução; 0 = sem teto
  maxCorpusBytes: Number(process.env.MAX_CORPUS_BYTES || 1_000_000),
  maxCorpusFiles: Number(process.env.MAX_CORPUS_FILES || 40),
  rateWindowMs: 60_000,
  rateMax: Number(process.env.RATE_MAX || 6),
};

/** US$ por 1M tokens — CONFIRA e ajuste, preços mudam. */
const PRICES = {
  'claude-haiku-4-5': { in: 1.00, out: 5.00 },
  'claude-sonnet-4': { in: 3.00, out: 15.00 },
  'claude-opus-4': { in: 15.00, out: 75.00 },
  'gemini-2.5-flash': { in: 0.30, out: 2.50 },
  'gemini-2.5-pro': { in: 1.25, out: 10.00 },
};
const priceOf = (m) => PRICES[Object.keys(PRICES).find((k) => m.startsWith(k))] || { in: 0, out: 0 };
const costOf = (m, tin, tout) => (tin / 1e6) * priceOf(m).in + (tout / 1e6) * priceOf(m).out;

/* ------------------------------------------- protocolo hexadecimal (URB1) */
const MAGIC = '55524231';            // "URB1"
const MAGIC_LEGADO = '504C5831';     // "PLX1" — quadros e dossiês da versão anterior
const PHASE = { SCATTER: 0x01, XREF: 0x02, INTERP: 0x03, GOSSIP: 0x04, REDUCE: 0x05, EVIDENCE: 0x06, ARBITER: 0x07 };
const PHASE_NAME = { 1: 'SCATTER', 2: 'XREF', 3: 'INTERP', 4: 'GOSSIP', 5: 'REDUCE', 6: 'EVIDENCE', 7: 'ARBITER' };

const toHex = (s) => Buffer.from(String(s), 'utf8').toString('hex').toUpperCase();
const fromHex = (h) => Buffer.from(String(h).replace(/[^0-9a-fA-F]/g, ''), 'hex').toString('utf8');
const b = (n) => (n & 0xff).toString(16).padStart(2, '0').toUpperCase();
const w = (n) => (n & 0xffff).toString(16).padStart(4, '0').toUpperCase();

function crc16(hexPayload) {
  const buf = Buffer.from(hexPayload, 'hex');
  let crc = 0xffff;
  for (const byte of buf) {
    crc ^= byte << 8;
    for (let i = 0; i < 8; i++) crc = crc & 0x8000 ? ((crc << 1) ^ 0x1021) & 0xffff : (crc << 1) & 0xffff;
  }
  return w(crc);
}
function frame({ src, dst, phase, ttl = 4, payload }) {
  const hex = toHex(payload);
  const len = Math.min(hex.length / 2, 0xffff);
  return { hex: MAGIC + b(1) + b(src) + b(dst) + b(phase) + b(ttl) + w(len) + hex + crc16(hex), payload: String(payload), bytes: len, src, dst, phase };
}
function parseFrame(raw) {
  const h = String(raw).replace(/[^0-9A-Fa-f]/g, '').toUpperCase();
  if (!h.startsWith(MAGIC) && !h.startsWith(MAGIC_LEGADO)) throw new Error('quadro inválido: magic desconhecido');
  const len = parseInt(h.substr(18, 4), 16);
  const payloadHex = h.substr(22, len * 2);
  const crc = h.substr(22 + len * 2, 4);
  if (crc && crc !== crc16(payloadHex)) throw new Error('CRC divergente');
  return { src: parseInt(h.substr(10, 2), 16), dst: parseInt(h.substr(12, 2), 16), phase: parseInt(h.substr(14, 2), 16), fase: PHASE_NAME[parseInt(h.substr(14, 2), 16)], payload: fromHex(payloadHex) };
}
const neighbors = (i, n) => { const s = new Set([(i + 1) % n, (i - 1 + n) % n, (i + 3) % n]); s.delete(i); return [...s]; };

/* ------------------------------------------------------ perfis de domínio */
/* O sistema não é de nenhum assunto específico: o perfil troca as 12 lentes.
   `g:1` marca a lente que recebe evidência da camada de fontes por padrão. */
const PROFILES = {
  geral: {
    nome: 'Geral', desc: 'Investigação de propósito amplo',
    personas: [
      { tag: 'CÉTICO', dir: 'Procure o que falsificaria a tese. Diga exatamente o que a derrubaria.' },
      { tag: 'QUANTITATIVO', dir: 'Trabalhe em ordens de grandeza. Confira dimensões, unidades e números que não fecham.' },
      { tag: 'HISTÓRICO', dir: 'Situe na trajetória: o que já foi tentado, o que falhou, o que mudou desde então.', g: 1 },
      { tag: 'ESTRUTURAL', dir: 'Isole a forma: invariantes, simetrias, condições de contorno, acidente contra essência.' },
      { tag: 'ADVERSARIAL', dir: 'Assuma um oponente competente. Modos de falha e incentivos para burlar.' },
      { tag: 'EMPÍRICO', dir: 'Responda o que mediria: qual dado, com que instrumento, com que precisão encerra a dúvida.', g: 1 },
      { tag: 'SISTÊMICO', dir: 'Rastreie acoplamentos e efeitos de segunda ordem. Onde o ótimo local piora o global.' },
      { tag: 'ECONÔMICO', dir: 'Custo, incentivo e viabilidade. Quem paga, quem ganha, o que só existe subsidiado.' },
      { tag: 'ANALÓGICO', dir: 'Ache o isomorfismo com outro domínio e teste onde a analogia quebra. A quebra é o achado.' },
      { tag: 'OPERACIONAL', dir: 'Traduza em ação de uma semana: sequência, pré-requisito, critério de parada.' },
      { tag: 'EPISTÊMICO', dir: 'Separe o que se sabe, o que se infere e o que se supõe. Atribua confiança a cada bloco.' },
      { tag: 'CONTRAFACTUAL', dir: 'Negue a premissa central e desenvolva o mundo resultante. O que sobrevive é sólido.' },
    ],
  },
  software: {
    nome: 'Software', desc: 'Arquitetura, operação e evolução de sistemas',
    personas: [
      { tag: 'ARQUITETURA', dir: 'Fronteiras de módulo, acoplamento, o que fica dentro e fora do processo.' },
      { tag: 'DESEMPENHO', dir: 'Caminho quente, complexidade, alocação, latência de cauda. Onde o gargalo realmente está.', g: 1 },
      { tag: 'CONFIABILIDADE', dir: 'Modos de falha parcial, timeout, retry, idempotência, o que acontece quando metade cai.' },
      { tag: 'SEGURANÇA', dir: 'Superfície exposta, validação de entrada, segredo em repouso e em trânsito, menor privilégio.' },
      { tag: 'DADOS', dir: 'Modelo, integridade referencial, migração, retenção, o que é fonte da verdade.' },
      { tag: 'CONTRATO', dir: 'Interface pública, versionamento, compatibilidade, o que quebra o consumidor.' },
      { tag: 'TESTABILIDADE', dir: 'O que dá para provar automaticamente e o que só se descobre em produção.' },
      { tag: 'OPERAÇÃO', dir: 'Implantação, observabilidade, reversão, plantão. Como se enxerga que quebrou.' },
      { tag: 'CUSTO', dir: 'Custo de infra e de chamada externa por unidade de uso. O que escala em dinheiro.', g: 1 },
      { tag: 'DÍVIDA', dir: 'O que hoje é atalho e vira travamento. O que pagar agora e o que deixar apodrecer de propósito.' },
      { tag: 'EXPERIÊNCIA', dir: 'O caminho do usuário: passo desnecessário, estado sem retorno, erro sem saída.' },
      { tag: 'MIGRAÇÃO', dir: 'Caminho do estado atual ao proposto sem parar o serviço. Passo reversível a passo reversível.' },
    ],
  },
  seguranca: {
    nome: 'Segurança (defensiva)', desc: 'Defesa, detecção e resposta — sem tradecraft ofensivo',
    personas: [
      { tag: 'SUPERFÍCIE', dir: 'Inventário do que está exposto: porta, endpoint, dependência, integração de terceiro.' },
      { tag: 'AMEAÇA', dir: 'Modele adversários plausíveis por capacidade e motivação, não por cenário de filme.' },
      { tag: 'DETECÇÃO', dir: 'Que sinal denunciaria isso, em que log, com que taxa de falso positivo aceitável.', g: 1 },
      { tag: 'RESPOSTA', dir: 'Contenção, erradicação e comunicação. Quem decide e em quanto tempo.' },
      { tag: 'SUPRIMENTOS', dir: 'Dependências e sua procedência: build reprodutível, assinatura, atualização.' },
      { tag: 'CRIPTOGRAFIA', dir: 'Onde há cifra, qual, com que chave, gerada e guardada por quem, rotacionada quando.' },
      { tag: 'IDENTIDADE', dir: 'Autenticação, autorização, sessão, privilégio residual, conta órfã.' },
      { tag: 'AUDITORIA', dir: 'O que fica registrado, por quanto tempo, com que integridade, e quem pode apagar.' },
      { tag: 'CONFORMIDADE', dir: 'Obrigação legal e contratual aplicável — LGPD e equivalentes — e a prova exigida.', g: 1 },
      { tag: 'RESILIÊNCIA', dir: 'Degradação controlada sob ataque ou perda. O que continua funcionando.' },
      { tag: 'FATOR HUMANO', dir: 'Onde o processo depende de alguém não errar sob pressão. Engenharia social e fadiga de alerta.' },
      { tag: 'RECUPERAÇÃO', dir: 'Backup testado, RPO/RTO reais, e o ensaio que ninguém fez.' },
    ],
  },
  ciencia: {
    nome: 'Ciência', desc: 'Hipótese, método, evidência',
    personas: [
      { tag: 'HIPÓTESE', dir: 'Enuncie a afirmação de forma falsificável. O que exatamente está sendo dito.' },
      { tag: 'MÉTODO', dir: 'Desenho do estudo, controle, cegamento, confusão. O método sustenta a conclusão?' },
      { tag: 'DADOS', dir: 'Origem, tamanho, seleção, dado ausente, o que foi descartado e por quê.', g: 1 },
      { tag: 'ESTATÍSTICA', dir: 'Poder, tamanho de efeito, incerteza, múltiplas comparações, p-hacking.' },
      { tag: 'REPRODUÇÃO', dir: 'Alguém repetiu? Com que independência? O que o material permite reproduzir.', g: 1 },
      { tag: 'LITERATURA', dir: 'Estado da arte, resultado conflitante, consenso e sua idade.', g: 1 },
      { tag: 'VIÉS', dir: 'Publicação, financiamento, sobrevivência, expectativa do observador.' },
      { tag: 'MECANISMO', dir: 'Existe caminho causal plausível? Correlação sem mecanismo é sinal de alerta.' },
      { tag: 'ESCALA', dir: 'O resultado vale em que faixa de parâmetro, e onde deixa de valer.' },
      { tag: 'INSTRUMENTO', dir: 'Limite de medição, calibração, artefato do aparelho confundido com fenômeno.' },
      { tag: 'ALTERNATIVA', dir: 'Qual outra explicação cobre os mesmos dados com menos suposição.' },
      { tag: 'IMPLICAÇÃO', dir: 'Se for verdade, o que mais precisa ser verdade — e isso é observado?' },
    ],
  },
  matematica: {
    nome: 'Matemática', desc: 'Enunciado, estrutura, obstrução',
    personas: [
      { tag: 'ENUNCIADO', dir: 'Formalize. Quantificadores, domínio, hipótese e tese explícitos, sem ambiguidade.' },
      { tag: 'CASOS PEQUENOS', dir: 'Calcule n=1,2,3… à mão. Padrão que aparece cedo e padrão que some depois.' },
      { tag: 'INVARIANTE', dir: 'Procure a quantidade preservada, paridade, monovariante, coloração.' },
      { tag: 'CONTRAEXEMPLO', dir: 'Tente quebrar. Caso degenerado, fronteira, hipótese silenciosamente usada.' },
      { tag: 'GENERALIZAÇÃO', dir: 'A versão mais forte é mais fácil? Enfraquecer hipótese revela a estrutura real.' },
      { tag: 'ESTRUTURA', dir: 'Que objeto algébrico, topológico ou combinatório está por trás disso.' },
      { tag: 'COTAS', dir: 'Estimativas superior e inferior. Onde a desigualdade é justa e onde é folgada.' },
      { tag: 'MÉTODO', dir: 'Técnica padrão aplicável e por que ela funciona ou não funciona neste caso.', g: 1 },
      { tag: 'OBSTRUÇÃO', dir: 'A barreira conhecida que impede o ataque ingênuo. Nomeie-a e diga por que ela morde.' },
      { tag: 'ANÁLOGO', dir: 'Problema resolvido com forma parecida e o que a tradução preserva ou perde.', g: 1 },
      { tag: 'RIGOR', dir: 'Audite os passos. Onde há salto, "claramente", ou troca ilegal de limite e soma.' },
      { tag: 'COMPUTACIONAL', dir: 'O que verificar por máquina, em que faixa, e que evidência numérica valeria.' },
    ],
  },
  negocio: {
    nome: 'Negócio', desc: 'Proposta, mercado, execução',
    personas: [
      { tag: 'PROBLEMA', dir: 'Que dor real existe sem isso, e quão cara ela é hoje para quem a sente.' },
      { tag: 'CLIENTE', dir: 'Quem paga, quem usa, quem decide. Se forem pessoas diferentes, isso muda tudo.' },
      { tag: 'MERCADO', dir: 'Tamanho alcançável de verdade, não o total. Como se chega ao primeiro cliente.', g: 1 },
      { tag: 'RECEITA', dir: 'Modelo de cobrança, ticket, recorrência, o que faz a conta fechar.' },
      { tag: 'CUSTO', dir: 'Custo unitário de servir, margem e o que sobe junto com o volume.' },
      { tag: 'CONCORRÊNCIA', dir: 'Alternativa atual, inclusive planilha e não fazer nada. Por que trocariam.', g: 1 },
      { tag: 'REGULATÓRIO', dir: 'Licença, tributo, obrigação fiscal e de dados que incidem sobre a operação.', g: 1 },
      { tag: 'EXECUÇÃO', dir: 'Quem faz, com que capacidade instalada, em que prazo verificável.' },
      { tag: 'RISCO', dir: 'O que mata isso em 12 meses e qual o sinal precoce de que está acontecendo.' },
      { tag: 'MÉTRICA', dir: 'O número que se olha toda semana, e como ele pode ser manipulado sem gerar valor.' },
      { tag: 'ESCALA', dir: 'O que quebra quando multiplica por dez: processo, suporte, entrega, caixa.' },
      { tag: 'CONTINUIDADE', dir: 'Dependência de pessoa, cliente ou fornecedor único. O que sobrevive à saída dele.' },
    ],
  },
};
const profileOf = (k) => PROFILES[k] || PROFILES.geral;

/* Catálogo achatado: cada lente ganha uma chave "perfil:índice" para composição livre.
   É o que permite montar 4 lentes de software + 4 de segurança + 4 de negócio. */
const CATALOGO = Object.entries(PROFILES).flatMap(([chave, p]) =>
  p.personas.map((x, i) => ({ chave: `${chave}:${i}`, perfil: chave, perfilNome: p.nome, tag: x.tag, dir: x.dir, g: x.g || null })));
const lenteDe = (chave) => CATALOGO.find((l) => l.chave === chave) || null;

/** Resolve as lentes da execução: composição explícita ganha do perfil. */
function montarLentes(opts) {
  const pedidas = Array.isArray(opts.lentes) ? opts.lentes.map(lenteDe).filter(Boolean) : [];
  if (pedidas.length) {
    const n = Math.max(1, Math.min(16, opts.clusters || pedidas.length));
    return { personas: pedidas.slice(0, n), composto: true, nome: 'Composto', desc: `${new Set(pedidas.slice(0, n).map((l) => l.perfil)).size} perfis combinados` };
  }
  const perfil = profileOf(opts.perfil);
  const n = Math.max(1, Math.min(perfil.personas.length, opts.clusters || CFG.clusters));
  return { personas: perfil.personas.slice(0, n), composto: false, nome: perfil.nome, desc: perfil.desc };
}

/** Fonte de evidência de um cluster: override explícito > marca da lente > global. */
function fonteDaLente(i, persona, opts, global) {
  const explicito = opts.fontesLente && opts.fontesLente[i];
  if (explicito) return SOURCES[explicito] && explicito !== 'nenhuma' ? explicito : null;
  if (!global || global === 'nenhuma') return null;
  if (opts.fonteTodos) return global;
  if (!persona.g) return null;
  return typeof persona.g === 'string' && SOURCES[persona.g] ? persona.g : global;
}

/* --------------------------------------------------- camada de fontes */
/* Adaptadores independentes de assunto. `http` é o ponto de extensão:
   aponte SOURCE_HTTP para qualquer API — catálogo, base de CVE, ERP interno. */
const SOURCES = {
  nenhuma: { tag: 'NENHUMA', desc: 'Sem coleta externa' },
  web: { tag: 'WEB', desc: 'Busca do Gemini com grounding' },
  http: { tag: 'HTTP', desc: 'Endpoint JSON em SOURCE_HTTP (use {q})' },
  corpus: { tag: 'CORPUS', desc: 'Arquivos .txt/.md em CORPUS_DIR' },
};

function httpGet(urlStr) {
  return new Promise((resolve, reject) => {
    const u = new URL(urlStr);
    const mod = u.protocol === 'http:' ? http : https;
    const req = mod.get(u, { headers: { accept: 'application/json,text/plain,*/*', 'user-agent': 'uroboros/4' } }, (res) => {
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) { res.resume(); return resolve(httpGet(new URL(res.headers.location, u).href)); }
      let d = ''; res.setEncoding('utf8');
      res.on('data', (c) => { d += c; if (d.length > 120_000) req.destroy(); });
      res.on('end', () => resolve({ status: res.statusCode, body: d }));
    });
    req.on('error', reject);
    req.setTimeout(20_000, () => req.destroy(new Error('timeout da fonte')));
  });
}

async function sourceCorpus(query) {
  let files;
  try { files = (await fsp.readdir(CFG.corpusDir)).filter((f) => /\.(txt|md|json|csv|log)$/i.test(f)); }
  catch { return { texto: '', refs: [], nota: 'CORPUS_DIR inexistente' }; }
  if (!files.length) return { texto: '', refs: [], nota: 'corpus vazio' };
  const termos = [...tokens(query)];
  const achados = [];
  for (const f of files.slice(0, 60)) {
    let txt;
    try { txt = await fsp.readFile(path.join(CFG.corpusDir, f), 'utf8'); } catch { continue; }
    const paras = txt.split(/\n\s*\n/).filter((p) => p.trim().length > 60);
    for (const p of paras) {
      const t = tokens(p);
      const score = termos.reduce((a, x) => a + (t.has(x) ? 1 : 0), 0);
      if (score >= 2) achados.push({ f, score, p: p.trim().slice(0, 700) });
    }
  }
  achados.sort((a, b) => b.score - a.score);
  const top = achados.slice(0, 6);
  return {
    texto: top.map((a) => `[${a.f}] ${a.p}`).join('\n\n'),
    refs: [...new Set(top.map((a) => a.f))].map((f) => ({ titulo: f, uri: `corpus://${f}` })),
    nota: top.length ? '' : 'nenhum trecho relevante no corpus',
  };
}

async function sourceHttp(query) {
  if (!CFG.httpSource) return { texto: '', refs: [], nota: 'SOURCE_HTTP não configurada' };
  const url = CFG.httpSource.includes('{q}')
    ? CFG.httpSource.replace('{q}', encodeURIComponent(query))
    : CFG.httpSource + encodeURIComponent(query);
  const r = await httpGet(url);
  if (r.status !== 200) return { texto: '', refs: [], nota: `fonte HTTP respondeu ${r.status}` };
  let corpo = r.body;
  try { corpo = JSON.stringify(JSON.parse(r.body), null, 1); } catch {}
  return { texto: corpo.slice(0, 6000), refs: [{ titulo: new URL(url).host, uri: url }], nota: '' };
}

async function sourceWeb(query, onWarn) {
  const r = await callGemini({
    system: 'Você é um coletor de evidência. Busque e devolva apenas fatos com data e origem, em tópicos curtos. Nada de opinião ou conclusão. Se não achar, diga que não achou. Português do Brasil.',
    user: `Colete evidência factual sobre:\n${query}`,
    maxTokens: 900, tools: [{ google_search: {} }], label: 'fonte web', onWarn,
  });
  return { texto: r.text, refs: r.refs || [], nota: '', uso: r };
}

async function gather(kind, query, onWarn) {
  if (!kind || kind === 'nenhuma') return null;
  if (kind === 'corpus') return sourceCorpus(query);
  if (kind === 'http') return sourceHttp(query);
  if (kind === 'web') return sourceWeb(query, onWarn);
  return null;
}

/* --------------------------------------------------------------- prompts */
const WIRE_SPEC = `PROTOCOLO URB1 (barramento hexadecimal)
Toda mensagem chega como um quadro hexadecimal contínuo:
  55524231 | VER(1B) | SRC(1B) | DST(1B) | FASE(1B) | TTL(1B) | LEN(2B) | PAYLOAD | CRC16-CCITT(2B)
O PAYLOAD é UTF-8 codificado em hexadecimal. Decodifique-o antes de raciocinar.
Fases: 01 SCATTER, 02 XREF, 03 INTERP, 04 GOSSIP, 05 REDUCE, 06 EVIDENCE, 07 ARBITER.
Responda SEMPRE em texto claro — o barramento re-codifica na saída. Português do Brasil.`;

const SYS_SCATTER = `${WIRE_SPEC}

Você é o ESCALONADOR do UROBOROS. Decompõe a consulta em eixos de investigação ortogonais,
um por cluster, sem sobreposição e sem repetir o enunciado. Cada cluster tem uma lente fixa:
adapte o eixo à lente correspondente, na ordem em que forem listadas. Se a consulta não tocar
o assunto de uma lente, invente o ângulo mais próximo que ainda seja útil — nenhum cluster ocioso.
Responda EXCLUSIVAMENTE com JSON, sem cercas de código:
{"eixos":[{"id":0,"titulo":"3 a 5 palavras","tarefa":"1 frase imperativa e específica"}]}`;

const sysXref = (p) => `${WIRE_SPEC}

Você é o núcleo CRUZAMENTO (Claude) do cluster ${p.tag} no UROBOROS.
Função geral: análise estrutural e cruzamento determinístico — decompor, quantificar, isolar
premissas, separar verificável de conjectura. Nunca invente número, fonte ou citação; marque
falta de dado com [LACUNA: ...]. Se receber um bloco EVIDÊNCIA, use-o e diga quando ele
contradiz sua análise; evidência ausente não vira suposição.
Sua lente obrigatória (${p.tag}): ${p.dir}
Máximo 220 palavras, denso, sem preâmbulo. Linhas curtas iniciadas por "•".
Encerre com "DIGEST: <=200 caracteres" com o achado central, não um resumo genérico.`;

const sysInterp = (p) => `${WIRE_SPEC}

Você é o núcleo INTERPRETAÇÃO (Gemini) do cluster ${p.tag} no UROBOROS.
Recebe o quadro XREF do seu par. Função: contextualizar, ampliar, levantar contra-hipóteses e
sinalizar o que exigiria dado externo. Não repita o XREF — acrescente ou discorde dele.
Mantenha a lente do cluster (${p.tag}): ${p.dir}
Máximo 200 palavras. Linhas curtas iniciadas por "→".
Encerre com "DIGEST: <=200 caracteres" e "CONFIANCA: 0.00-1.00", onde a confiança reflete
quanto do seu achado se sustenta em dado verificável, não quanto você gostou dele.`;

const sysRefine = (p, round) => `${WIRE_SPEC}

Você é o CRUZAMENTO do cluster ${p.tag}, rodada ${round} de GOSSIP.
Chegaram digests dos clusters vizinhos. Reconcilie: o que confirmam, o que contradizem, o que
torna seu achado redundante. Se nada relevante mudou, diga isso em uma linha e repita seu digest —
estabilidade é informação, não é fracasso. Mantenha a lente ${p.tag}. Máximo 120 palavras.
Encerre com "DIGEST: <=200 caracteres".`;

const SYS_ARBITER = `${WIRE_SPEC}

Você é o ÁRBITRO do UROBOROS. Recebe pares de clusters cujos digests divergem materialmente,
detectados por baixa similaridade léxica. Para cada par: diga se é divergência real (afirmações
incompatíveis), divergência aparente (recortes diferentes do mesmo fato) ou complementaridade.
Quando for real, aponte qual lado tem base mais verificável e o que decidiria a questão.
Não invente dado para desempatar. Máximo 60 palavras por par. Formato:
"C0x × C0y — [REAL|APARENTE|COMPLEMENTAR] — veredito"`;

const SYS_REDUCE = `${WIRE_SPEC}

Você é o REDUTOR do UROBOROS. Recebe os quadros finais de todos os clusters, cada um com uma
lente distinta, mais o parecer do árbitro quando houver.
Regras: integre, não concatene. Nomeie divergências citando o identificador (C03 contra C07).
Separe conclusão de conjectura. Liste as lacunas que impedem fechamento. Registre cluster offline.
Não introduza fato que não esteja no dossiê.
Linhas VERIFICADOR são auditoria determinística já executada sobre o texto dos clusters, não
opinião: onde houver [ERRO], descarte ou corrija a afirmação correspondente em vez de repeti-la,
e registre a correção em Lacunas. Português do Brasil, markdown enxuto.
Estrutura: ## Síntese / ## Convergências / ## Divergências / ## Lacunas / ## Próximo passo`;

/* ------------------------------------------------------ transporte HTTPS */
function streamRequest(opts, body, onLine) {
  return new Promise((resolve, reject) => {
    const req = https.request(opts, (res) => {
      let buf = '', raw = '';
      res.setEncoding('utf8');
      res.on('data', (chunk) => {
        raw += chunk;
        if (res.statusCode !== 200) return;
        buf += chunk;
        let i;
        while ((i = buf.indexOf('\n')) >= 0) { onLine(buf.slice(0, i).trim()); buf = buf.slice(i + 1); }
      });
      res.on('end', () => resolve({ status: res.statusCode, body: raw }));
    });
    req.on('error', reject);
    req.setTimeout(180_000, () => req.destroy(new Error('timeout de 180s')));
    if (body) req.write(body);
    req.end();
  });
}

const RETRIABLE = /\b(429|500|502|503|529|overloaded|timeout|ECONNRESET|socket hang up)\b/i;
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function withRetry(fn, label, onWarn) {
  let last;
  for (let attempt = 0; attempt <= CFG.retries; attempt++) {
    const state = { emitted: false };
    try { return await fn(state); }
    catch (e) {
      last = e;
      if (state.emitted || attempt === CFG.retries || !RETRIABLE.test(e.message)) break;
      const wait = Math.round(500 * 2 ** attempt + Math.random() * 300);
      onWarn?.(`${label}: ${e.message} — nova tentativa em ${wait}ms`);
      await sleep(wait);
    }
  }
  throw last;
}

async function callClaude({ system, user, maxTokens = CFG.maxTokens, model = CFG.claudeModel, onDelta, onWarn, label = 'claude' }) {
  if (!CFG.anthropicKey) throw new Error('ANTHROPIC_API_KEY não configurada');
  return withRetry(async (state) => {
    const body = JSON.stringify({ model, max_tokens: maxTokens, stream: true, system, messages: [{ role: 'user', content: user }] });
    const t0 = Date.now();
    let text = '', tin = 0, tout = 0, apiErr = null;
    const res = await streamRequest({
      hostname: 'api.anthropic.com', path: '/v1/messages', method: 'POST',
      headers: { 'content-type': 'application/json', 'x-api-key': CFG.anthropicKey, 'anthropic-version': '2023-06-01', 'content-length': Buffer.byteLength(body) },
    }, body, (line) => {
      if (!line.startsWith('data:')) return;
      let ev; try { ev = JSON.parse(line.slice(5).trim()); } catch { return; }
      if (ev.type === 'message_start') tin = ev.message?.usage?.input_tokens || 0;
      else if (ev.type === 'content_block_delta' && ev.delta?.type === 'text_delta') { text += ev.delta.text; state.emitted = true; onDelta?.(ev.delta.text); }
      else if (ev.type === 'message_delta') tout = ev.usage?.output_tokens || tout;
      else if (ev.type === 'error') apiErr = ev.error?.message || 'erro de stream';
    });
    if (res.status !== 200) {
      let m = res.body.slice(0, 200);
      try { m = JSON.parse(res.body).error.message; } catch {}
      throw new Error(`Claude ${res.status}: ${m}`);
    }
    if (apiErr) throw new Error(`Claude: ${apiErr}`);
    return { text: text.trim(), ms: Date.now() - t0, in: tin, out: tout, model, cost: costOf(model, tin, tout) };
  }, label, onWarn);
}

async function callGemini({ system, user, maxTokens = CFG.maxTokens, tools, onDelta, onWarn, label = 'gemini' }) {
  if (!CFG.geminiKey) throw new Error('GEMINI_API_KEY não configurada');
  return withRetry(async (state) => {
    const payload = {
      systemInstruction: { parts: [{ text: system }] },
      contents: [{ role: 'user', parts: [{ text: user }] }],
      generationConfig: { temperature: 0.7, maxOutputTokens: maxTokens },
    };
    if (tools) payload.tools = tools;
    const body = JSON.stringify(payload);
    const t0 = Date.now();
    let text = '', tin = 0, tout = 0;
    const refs = [];
    const res = await streamRequest({
      hostname: 'generativelanguage.googleapis.com',
      path: `/v1beta/models/${encodeURIComponent(CFG.geminiModel)}:streamGenerateContent?alt=sse&key=${encodeURIComponent(CFG.geminiKey)}`,
      method: 'POST', headers: { 'content-type': 'application/json', 'content-length': Buffer.byteLength(body) },
    }, body, (line) => {
      if (!line.startsWith('data:')) return;
      let ev; try { ev = JSON.parse(line.slice(5).trim()); } catch { return; }
      for (const p of ev.candidates?.[0]?.content?.parts || []) if (p.text) { text += p.text; state.emitted = true; onDelta?.(p.text); }
      for (const g of ev.candidates?.[0]?.groundingMetadata?.groundingChunks || []) {
        if (g.web?.uri && !refs.some((r) => r.uri === g.web.uri)) refs.push({ titulo: g.web.title || g.web.uri, uri: g.web.uri });
      }
      if (ev.usageMetadata) { tin = ev.usageMetadata.promptTokenCount || tin; tout = ev.usageMetadata.candidatesTokenCount || tout; }
    });
    if (res.status !== 200) {
      let m = res.body.slice(0, 200);
      try { m = JSON.parse(res.body).error.message; } catch {}
      throw new Error(`Gemini ${res.status}: ${m}`);
    }
    return { text: text.trim(), ms: Date.now() - t0, in: tin, out: tout, model: CFG.geminiModel, cost: costOf(CFG.geminiModel, tin, tout), refs };
  }, label, onWarn);
}

/* --------------------------------------------------------- convergência */
function tokens(s) {
  return new Set(String(s).toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g, '').match(/[a-z0-9]{4,}/g) || []);
}
function jaccard(a, b) {
  const A = tokens(a), B = tokens(b);
  if (!A.size && !B.size) return 1;
  let inter = 0;
  for (const t of A) if (B.has(t)) inter++;
  return inter / (A.size + B.size - inter || 1);
}
const digestOf = (t) => (String(t).match(/DIGEST:\s*(.+)/i) || [, ''])[1].trim().slice(0, 200);
const confOf = (t) => { const m = String(t).match(/CONFIANCA:\s*([0-9.]+)/i); return m ? Math.min(1, parseFloat(m[1]) || 0) : null; };

/* ------------------------------------------ verificador determinístico */
/* Auditoria sem LLM sobre a saída dos clusters. Não julga mérito: só aponta
   o que é aritmeticamente, cronologicamente ou dimensionalmente insustentável. */

const RE_NUM = String.raw`-?\d[\d.\u00a0 ]*(?:,\d+)?|-?\d+(?:\.\d+)?`;
function num(s) {
  let t = String(s).replace(/[\s\u00a0]/g, '');
  if (/,\d+$/.test(t)) t = t.replace(/\./g, '').replace(',', '.');
  else if (/\.\d{3}(?!\d)/.test(t) && !/\.\d{1,2}$/.test(t)) t = t.replace(/\./g, '');
  const v = Number(t);
  return Number.isFinite(v) ? v : null;
}
const proximo = (a, b) => Math.abs(a - b) <= Math.max(Math.abs(b) * 0.005, 0.01);
const corte = (s, i, n = 70) => String(s).slice(Math.max(0, i - 10), i + n).replace(/\s+/g, ' ').trim();

const PREFIXO = { '': 1, k: 1e3, K: 1e3, M: 1e6, G: 1e9, T: 1e12, m: 1e-3, µ: 1e-6, u: 1e-6, n: 1e-9 };
const BASE = ['B', 'Hz', 'W', 'm', 'g', 's', 'bps', 'FLOPS', 'Wh', 'J', 'V', 'A'];

function verificar(c) {
  const achados = [];
  const txt = [c.xref, c.interp, ...(c.refinos || []).map((r) => r.text)].filter(Boolean).join('\n');
  if (!txt) return achados;
  const add = (tipo, sev, trecho, nota) => achados.push({ tipo, sev, trecho, nota });

  /* 1. aritmética explícita */
  for (const m of txt.matchAll(new RegExp(String.raw`(${RE_NUM})\s*([+\-×x*÷/])\s*(${RE_NUM})\s*=\s*(${RE_NUM})`, 'g'))) {
    const a = num(m[1]), b = num(m[3]), r = num(m[4]);
    if (a == null || b == null || r == null) continue;
    const op = m[2];
    const esperado = op === '+' ? a + b : op === '-' ? a - b : /[×x*]/.test(op) ? a * b : b !== 0 ? a / b : null;
    if (esperado != null && !proximo(r, esperado)) {
      add('aritmética', 'erro', corte(txt, m.index), `resultado declarado ${r}, calculado ${Number(esperado.toFixed(6))}`);
    }
  }

  /* 2. intervalos invertidos */
  for (const m of txt.matchAll(new RegExp(String.raw`\b(?:entre|de)\s+(?:R\$\s*)?(${RE_NUM})\s+(?:e|a|até)\s+(?:R\$\s*)?(${RE_NUM})`, 'gi'))) {
    const a = num(m[1]), b = num(m[2]);
    if (a != null && b != null && a > b) add('intervalo', 'erro', corte(txt, m.index), `limite inferior ${a} maior que o superior ${b}`);
  }

  /* 3. datas impossíveis e períodos invertidos */
  for (const m of txt.matchAll(/\b(\d{1,2})[\/.-](\d{1,2})[\/.-](\d{4})\b/g)) {
    const [d, mo, y] = [+m[1], +m[2], +m[3]];
    const dt = new Date(Date.UTC(y, mo - 1, d));
    if (mo < 1 || mo > 12 || dt.getUTCDate() !== d || dt.getUTCMonth() !== mo - 1) {
      add('data', 'erro', corte(txt, m.index), `data inexistente no calendário`);
    }
  }
  for (const m of txt.matchAll(/\b(?:de|entre)\s+(\d{4})\s+(?:a|e|até)\s+(\d{4})\b/g)) {
    if (+m[1] > +m[2]) add('período', 'erro', corte(txt, m.index), `período começa depois de terminar`);
  }

  /* 4. igualdades dimensionais */
  const un = `(?:${BASE.join('|')})`;
  for (const m of txt.matchAll(new RegExp(String.raw`(${RE_NUM})\s*([kKMGTmµun]?)(${un})\b\s*=\s*(${RE_NUM})\s*([kKMGTmµun]?)(${un})\b`, 'g'))) {
    const a = num(m[1]), b = num(m[4]);
    if (a == null || b == null) continue;
    if (m[3] !== m[6]) { add('unidade', 'erro', corte(txt, m.index), `igualdade entre grandezas diferentes (${m[3]} e ${m[6]})`); continue; }
    const esq = a * (PREFIXO[m[2]] ?? 1), dir = b * (PREFIXO[m[5]] ?? 1);
    const binario = m[3] === 'B' && (proximo(esq / dir, (1024 / 1000) ** 1) || proximo(dir / esq, (1024 / 1000) ** 1) || proximo(esq / dir, (1024 / 1000) ** 2) || proximo(dir / esq, (1024 / 1000) ** 2));
    if (!proximo(esq, dir) && !binario) {
      add('unidade', 'erro', corte(txt, m.index), `conversão não fecha: ${esq} contra ${dir} na unidade base`);
    }
  }

  /* 5. percentual fora de escala sem contexto de variação */
  for (const m of txt.matchAll(new RegExp(String.raw`(${RE_NUM})\s*%`, 'g'))) {
    const v = num(m[1]);
    if (v == null) continue;
    const antes = txt.slice(Math.max(0, m.index - 60), m.index).toLowerCase();
    const variacao = /(aument|cresc|alta|queda|redu|varia|cai|sobe|acima|abaixo|margem|multipl)/.test(antes);
    if (v > 100 && !variacao) add('percentual', 'alerta', corte(txt, m.index), `${v}% como participação — acima de 100 sem indicar variação`);
    if (v < 0) add('percentual', 'alerta', corte(txt, m.index), `percentual negativo sem contexto`);
  }

  /* 6. URL sem lastro na coleta */
  const refs = (c.refs || []).map((r) => String(r.uri || ''));
  for (const m of txt.matchAll(/https?:\/\/[^\s)<>\]]+/g)) {
    const u = m[0].replace(/[.,;]$/, '');
    if (!refs.some((r) => r.includes(u) || u.includes(r.replace(/^https?:\/\//, '').split('/')[0]))) {
      add('citação', 'alerta', u.slice(0, 90), refs.length ? 'URL não está entre as fontes coletadas' : 'URL citada sem nenhuma coleta externa nesta execução');
    }
  }

  /* 7. confiança alta convivendo com lacuna declarada */
  const lacunas = (txt.match(/\[LACUNA/gi) || []).length;
  if (c.confianca != null && c.confianca >= 0.9 && lacunas > 0) {
    add('coerência', 'alerta', `confiança ${c.confianca} com ${lacunas} lacuna(s)`, 'confiança declarada não condiz com os dados ausentes apontados pelo próprio cluster');
  }

  return achados;
}

/* ------------------------------------------ verificação cruzada (sem LLM) */
/* O verificador anterior audita cada cluster isolado. Este compara clusters entre si:
   a mesma grandeza com valores incompatíveis, e afirmações que se negam. */

const VAZIAS = new Set(['para', 'como', 'pelo', 'pela', 'esse', 'essa', 'este', 'esta', 'isso', 'aqui', 'onde', 'quando', 'porque', 'sobre', 'entre', 'ainda', 'apenas', 'mesmo', 'cada', 'toda', 'todo', 'pode', 'deve', 'seja', 'está', 'esta', 'foram', 'sendo', 'muito', 'mais', 'menos', 'sem', 'com', 'que', 'nao', 'nas', 'nos', 'dos', 'das']);
const chaveCtx = (s) => [...new Set(String(s).toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g, '')
  .match(/[a-z]{4,}/g) || [])].filter((w) => !VAZIAS.has(w)).slice(-3);

/** extrai (contexto, valor normalizado à unidade base, unidade) de um texto */
function grandezas(txt) {
  const out = [];
  const un = `(?:${BASE.join('|')}|%)`;
  const re = new RegExp(String.raw`([^\n•→.;]{6,60}?)(${RE_NUM})\s*([kKMGTmµun]?)(${un})(?![a-zA-Z])`, 'g');
  for (const m of String(txt).matchAll(re)) {
    const v = num(m[2]);
    if (v == null) continue;
    const ctx = chaveCtx(m[1]);
    if (ctx.length < 1) continue;
    const base = m[4] === '%' ? '%' : m[4];
    out.push({ ctx, valor: v * (m[4] === '%' ? 1 : (PREFIXO[m[3]] ?? 1)), unidade: base, trecho: (m[1] + m[2] + (m[3] || '') + m[4]).trim().replace(/\s+/g, ' ') });
  }
  return out;
}

const simCtx = (a, b) => {
  const A = new Set(a), B = new Set(b);
  let i = 0;
  for (const t of A) if (B.has(t)) i++;
  return i / (A.size + B.size - i || 1);
};

function frasesDe(txt) {
  return String(txt).split(/[\n•→]+|(?<=[.;])\s+/).map((s) => s.trim())
    .filter((s) => s.length > 28 && !/^DIGEST|^CONFIANCA/i.test(s));
}
const semNegacao = (s) => s.replace(/\b(n[ãa]o|nunca|jamais|nenhum[ao]?|sem)\b/gi, ' ');
const temNegacao = (s) => /\b(n[ãa]o|nunca|jamais|nenhum[ao]?)\b/i.test(s);

function verificarCruzado(live) {
  const achados = [];
  const vivos = live.filter((c) => !c.erro);
  const textos = new Map(vivos.map((c) => [c.id, [c.xref, c.interp, ...(c.refinos || []).map((r) => r.text)].filter(Boolean).join('\n')]));

  /* 1. mesma grandeza, valores incompatíveis entre clusters */
  const medidas = vivos.flatMap((c) => grandezas(textos.get(c.id)).map((g) => ({ ...g, id: c.id })));
  const usados = new Set();
  for (let i = 0; i < medidas.length; i++) {
    if (usados.has(i)) continue;
    const grupo = [medidas[i]];
    for (let j = i + 1; j < medidas.length; j++) {
      if (usados.has(j) || medidas[j].unidade !== medidas[i].unidade || medidas[j].id === medidas[i].id) continue;
      if (simCtx(medidas[i].ctx, medidas[j].ctx) >= 0.5) { grupo.push(medidas[j]); usados.add(j); }
    }
    if (grupo.length < 2) continue;
    if (new Set(grupo.map((g) => g.id)).size < 2) continue;
    const vals = grupo.map((g) => Math.abs(g.valor)).filter((v) => v > 0);
    if (vals.length < 2) continue;
    const razao = Math.max(...vals) / Math.min(...vals);
    if (razao >= 3) {
      achados.push({
        tipo: 'grandeza divergente', sev: 'erro',
        clusters: [...new Set(grupo.map((g) => g.id))].sort((a, b) => a - b),
        nota: `mesma grandeza em ${grupo[0].unidade} varia ${razao.toFixed(1)}× entre clusters`,
        trecho: grupo.map((g) => `C${String(g.id).padStart(2, '0')}: ${g.trecho}`).join('  ·  ').slice(0, 220),
      });
      usados.add(i);
    }
  }

  /* 2. afirmação e sua negação em clusters diferentes */
  const fr = vivos.flatMap((c) => frasesDe(textos.get(c.id)).map((s) => ({ id: c.id, s })));
  const vistos = new Set();
  for (const a of fr.filter((x) => temNegacao(x.s))) {
    for (const b of fr.filter((x) => x.id !== a.id && !temNegacao(x.s))) {
      const par = `${Math.min(a.id, b.id)}-${Math.max(a.id, b.id)}`;
      if (vistos.has(par)) continue;
      if (jaccard(semNegacao(a.s), b.s) >= 0.55) {
        achados.push({
          tipo: 'contradição', sev: 'erro', clusters: [a.id, b.id].sort((x, y) => x - y),
          nota: 'um cluster nega o que o outro afirma nos mesmos termos',
          trecho: `C${String(a.id).padStart(2, '0')}: ${a.s.slice(0, 100)}  ×  C${String(b.id).padStart(2, '0')}: ${b.s.slice(0, 100)}`,
        });
        vistos.add(par);
        break;
      }
    }
  }

  return achados.slice(0, 10);
}

/* --------------------------------------------------------- orquestração */
async function pool(items, limit, fn, aborted) {
  const out = new Array(items.length);
  let idx = 0;
  await Promise.all(Array.from({ length: Math.min(limit, items.length) || 1 }, async () => {
    while (idx < items.length) {
      const i = idx++;
      if (aborted?.()) { out[i] = { ok: false, error: 'cancelado' }; continue; }
      try { out[i] = { ok: true, value: await fn(items[i], i) }; }
      catch (e) { out[i] = { ok: false, error: e.message }; }
    }
  }));
  return out;
}
const meta = (fr) => ({ src: fr.src, dst: fr.dst, phase: PHASE_NAME[fr.phase], bytes: fr.bytes, hex: fr.hex.length > 420 ? fr.hex.slice(0, 420) + '…' : fr.hex });
const wire = (fr, strict) => strict ? `QUADRO URB1:\n${fr.hex}` : `QUADRO URB1:\n${fr.hex}\n\n--- espelho decodificado (conferência) ---\n${fr.payload}`;

async function runJob(input, opts, emit, aborted) {
  const montagem = montarLentes(opts);
  const personas = montagem.personas;
  const n = personas.length;
  const strict = !!opts.strict;
  const maxRounds = Math.max(0, Math.min(CFG.maxRounds, opts.rounds ?? 0));
  const fonte = SOURCES[opts.fonte] ? opts.fonte : 'nenhuma';
  const usarArbitro = opts.arbitro !== false;
  const conc = Math.max(1, Math.min(10, opts.concurrency || CFG.concurrency));
  const jobId = crypto.randomBytes(5).toString('hex').toUpperCase();
  const t0 = Date.now();
  const acc = { in: 0, out: 0, calls: 0, cost: 0 };
  const warn = (message) => emit('warn', { message });

  /* teto de gasto: o rate limit segura volume, não segura um job caro */
  const teto = opts.tetoUSD != null ? Math.max(0, Number(opts.tetoUSD) || 0) : CFG.maxCostUSD;
  let estourou = false;
  const semSaldo = () => {
    if (!teto || acc.cost < teto) return false;
    if (!estourou) {
      estourou = true;
      emit('teto', { teto, gasto: acc.cost });
      warn(`teto de US$ ${teto.toFixed(2)} atingido com US$ ${acc.cost.toFixed(4)} — nada novo será iniciado, a síntese roda com o que já existe`);
    }
    return true;
  };
  const parar = () => aborted() || semSaldo();
  const bill = (r) => {
    acc.in += r.in; acc.out += r.out; acc.cost += r.cost; acc.calls++;
    emit('budget', { calls: acc.calls, tin: acc.in, tout: acc.out, cost: acc.cost, teto });
    return r;
  };

  const root = frame({ src: 0xff, dst: 0xff, phase: PHASE.SCATTER, payload: input });
  emit('job', {
    id: jobId, clusters: n, strict, rounds: maxRounds, teto,
    perfil: montagem.composto ? 'composto' : (opts.perfil || 'geral'), perfilNome: montagem.nome,
    lentes: personas.map((p) => ({ tag: p.tag, perfil: p.perfil || (opts.perfil || 'geral') })),
    fonte, hex: root.hex, bytes: root.bytes, models: { claude: CFG.claudeModel, gemini: CFG.geminiModel },
  });
  personas.forEach((p, i) => emit('cluster', {
    id: i, persona: p.tag, perfilLente: p.perfilNome || montagem.nome, status: 'ocioso',
  }));

  /* ── 01 SCATTER ── */
  emit('phase', { phase: 'SCATTER', code: '01' });
  let eixos = [];
  try {
    const lista = personas.map((p, i) => `${i}. ${p.tag} — ${p.dir}`).join('\n');
    const r = bill(await callClaude({
      system: SYS_SCATTER, maxTokens: 1100, label: 'escalonador', onWarn: warn,
      user: `${wire(root, strict)}\n\nPERFIL: ${montagem.nome} — ${montagem.desc}\nLENTES DOS CLUSTERS (na ordem):\n${lista}\n\nGere exatamente ${n} eixos.`,
    }));
    eixos = JSON.parse(r.text.replace(/```json|```/g, '').trim()).eixos.slice(0, n);
  } catch (e) { warn(`escalonador falhou (${e.message}) — partição por lente`); }
  while (eixos.length < n) eixos.push({ titulo: personas[eixos.length].tag, tarefa: personas[eixos.length].dir });
  eixos.forEach((e, i) => {
    e.id = i; e.persona = personas[i];
    const fr = frame({ src: 0xff, dst: i, phase: PHASE.SCATTER, payload: `${personas[i].tag} :: ${e.titulo}\n${e.tarefa}` });
    emit('cluster', { id: i, titulo: e.titulo, tarefa: e.tarefa, persona: personas[i].tag, status: 'armado' });
    emit('frame', meta(fr));
  });

  /* ── 06 EVIDENCE: cada lente pode ter a sua própria fonte ── */
  eixos.forEach((e, i) => { e.fonte = fonteDaLente(i, e.persona, opts, fonte); });
  let alvos = eixos.filter((e) => e.fonte);
  /* nenhum perfil garante lente aterrada nas N primeiras posições: com poucos clusters
     a coleta cairia no vazio. Aterra o primeiro cluster e avisa. */
  if (!alvos.length && fonte !== 'nenhuma' && eixos.length) {
    eixos[0].fonte = fonte;
    alvos = [eixos[0]];
    warn(`nenhuma lente aterrada entre as ${n} em uso — evidência coletada para C00 (${eixos[0].persona.tag})`);
  }
  if (alvos.length) {
    const usadas = [...new Set(alvos.map((e) => SOURCES[e.fonte].tag))].join('+');
    emit('phase', { phase: `EVIDENCE ${usadas}`, code: '06' });
    await pool(alvos, Math.min(conc, 3), async (e) => {
      emit('cluster', { id: e.id, status: 'coletando' });
      try {
        const q = `${input.slice(0, 300)} — ${e.titulo}: ${e.tarefa}`;
        const ev = await gather(e.fonte, q, warn);
        if (ev?.uso) bill(ev.uso);
        e.evidencia = ev?.texto || '';
        e.refs = ev?.refs || [];
        const fr = frame({ src: 0xfe, dst: e.id, phase: PHASE.EVIDENCE, payload: e.evidencia || ev?.nota || 'sem retorno' });
        emit('frame', meta(fr));
        emit('evidencia', { id: e.id, fonte: SOURCES[e.fonte].tag, texto: e.evidencia, refs: e.refs, nota: ev?.nota || '' });
      } catch (err) { warn(`fonte ${e.fonte} em C${e.id}: ${err.message}`); e.evidencia = ''; e.refs = []; }
    }, parar);
  }

  /* ── 02/03 XREF + INTERP ── */
  emit('phase', { phase: 'XREF·INTERP', code: '02/03' });
  const res = await pool(eixos, conc, async (eixo, i) => {
    emit('cluster', { id: i, status: 'cruzando' });
    const corpo = `CONSULTA:\n${input}\n\nSEU EIXO (${eixo.titulo}):\n${eixo.tarefa}`
      + (eixo.evidencia ? `\n\nEVIDÊNCIA COLETADA (${SOURCES[eixo.fonte].tag}):\n${eixo.evidencia.slice(0, 5000)}` : '');
    const fx = frame({ src: 0xff, dst: i, phase: PHASE.XREF, payload: corpo });
    const x = bill(await callClaude({
      system: sysXref(eixo.persona), user: wire(fx, strict), label: `C${i} xref`, onWarn: warn,
      onDelta: (t) => emit('delta', { id: i, side: 'xref', t }),
    }));
    const xf = frame({ src: i, dst: i, phase: PHASE.XREF, payload: x.text });
    emit('frame', meta(xf));
    emit('text', { id: i, side: 'xref', text: x.text, ms: x.ms, tin: x.in, tout: x.out, cost: x.cost });

    emit('cluster', { id: i, status: 'interpretando' });
    const g = bill(await callGemini({
      system: sysInterp(eixo.persona), user: wire(xf, strict), label: `C${i} interp`, onWarn: warn,
      onDelta: (t) => emit('delta', { id: i, side: 'interp', t }),
    }));
    const gf = frame({ src: i, dst: i, phase: PHASE.INTERP, payload: g.text });
    emit('frame', meta(gf));
    emit('text', { id: i, side: 'interp', text: g.text, ms: g.ms, tin: g.in, tout: g.out, cost: g.cost });

    const conf = confOf(g.text);
    emit('cluster', { id: i, status: maxRounds ? 'aguardando gossip' : 'pronto', confianca: conf });
    return { id: i, persona: eixo.persona.tag, perfilLente: eixo.persona.perfilNome || montagem.nome, titulo: eixo.titulo, xref: x.text, interp: g.text, refinos: [], evidencia: eixo.evidencia || '', refs: eixo.refs || [], fonte: eixo.fonte || '', digest: digestOf(g.text) || digestOf(x.text) || x.text.slice(0, 200), confianca: conf };
  }, parar);

  const live = res.map((r, i) => r.ok ? r.value
    : { id: i, persona: personas[i].tag, perfilLente: personas[i].perfilNome || montagem.nome, titulo: eixos[i].titulo, xref: '', interp: '', refinos: [], refs: [], digest: '', erro: r.error });
  live.forEach((c) => c.erro && emit('cluster', { id: c.id, status: 'falhou', erro: c.erro }));

  /* ── 04 GOSSIP com roteamento por confiança ── */
  const convergencia = [];
  for (let round = 1; round <= maxRounds; round++) {
    if (parar()) break;
    emit('phase', { phase: `GOSSIP r${round}`, code: '04' });
    const inbox = live.map(() => []);
    for (const c of live) {
      if (c.erro) continue;
      for (const nb of neighbors(c.id, n)) {
        const fr = frame({ src: c.id, dst: nb, phase: PHASE.GOSSIP, ttl: 1, payload: `${c.persona} :: ${c.digest}` });
        inbox[nb].push(fr.payload);
        emit('frame', meta(fr));
      }
    }
    /* na 1ª rodada todos reconciliam; a partir da 2ª, só quem está inseguro ou ainda instável */
    const fila = live.filter((c) => {
      if (c.erro) return false;
      if (round === 1) return true;
      const inseguro = c.confianca == null || c.confianca < CFG.confLimiar;
      const instavel = (c.ultimoDelta ?? 1) >= CFG.convergeAt;
      if (!inseguro && !instavel) { emit('cluster', { id: c.id, status: `estável (conf ${c.confianca.toFixed(2)})` }); return false; }
      return true;
    });
    if (!fila.length) { warn(`rodada ${round}: todos os clusters estáveis e confiantes — gossip encerrado`); break; }
    emit('roteamento', { round, ativos: fila.map((c) => c.id), dispensados: live.filter((c) => !c.erro && !fila.includes(c)).map((c) => c.id) });

    const antes = Object.fromEntries(live.map((c) => [c.id, c.digest]));
    await pool(fila, conc, async (c) => {
      emit('cluster', { id: c.id, status: `reconciliando r${round}` });
      const fr = frame({ src: 0xff, dst: c.id, phase: PHASE.GOSSIP, payload: `SEU ESTADO:\n${c.xref}\n\nDIGESTS RECEBIDOS:\n${inbox[c.id].join('\n')}` });
      const r = bill(await callClaude({
        system: sysRefine(personas[c.id], round), user: wire(fr, strict), maxTokens: 700,
        label: `C${c.id} refino r${round}`, onWarn: warn,
        onDelta: (t) => emit('delta', { id: c.id, side: 'refino', t }),
      }));
      c.refinos.push({ round, text: r.text });
      c.digest = digestOf(r.text) || c.digest;
      c.ultimoDelta = 1 - jaccard(antes[c.id], c.digest);
      emit('text', { id: c.id, side: 'refino', round, text: r.text, ms: r.ms, tin: r.in, tout: r.out, cost: r.cost });
      emit('cluster', { id: c.id, status: 'pronto' });
    }, parar);

    const deltas = fila.map((c) => c.ultimoDelta ?? 0);
    const delta = deltas.reduce((a, x) => a + x, 0) / (deltas.length || 1);
    convergencia.push({ round, delta: Number(delta.toFixed(3)), ativos: fila.length });
    emit('converge', { round, delta: Number(delta.toFixed(3)), limiar: CFG.convergeAt, ativos: fila.length });
    if (delta < CFG.convergeAt) { warn(`convergiu na rodada ${round} (Δ=${delta.toFixed(3)} < ${CFG.convergeAt})`); break; }
  }

  /* ── verificação determinística (sem LLM) ── */
  emit('phase', { phase: 'VERIFICAÇÃO', code: '--' });
  let totalAchados = 0, totalErros = 0;
  for (const c of live) {
    if (c.erro) continue;
    c.achados = verificar(c);
    totalAchados += c.achados.length;
    totalErros += c.achados.filter((a) => a.sev === 'erro').length;
    if (c.achados.length) emit('verificacao', { id: c.id, achados: c.achados });
  }
  const cruzados = verificarCruzado(live);
  totalAchados += cruzados.length;
  totalErros += cruzados.filter((a) => a.sev === 'erro').length;
  if (cruzados.length) emit('verificacao_cruzada', { achados: cruzados });
  emit('verificacao_total', { achados: totalAchados, erros: totalErros, cruzados: cruzados.length });
  if (totalErros) warn(`verificador: ${totalErros} inconsistência(s) dura(s), ${cruzados.length} delas entre clusters`);

  /* ── 07 ARBITER: divergências detectadas deterministicamente, julgadas depois ── */
  let arbitro = null;
  const vivos = live.filter((c) => !c.erro && c.digest);
  const pares = [];
  for (let i = 0; i < vivos.length; i++) for (let j = i + 1; j < vivos.length; j++) {
    const sim = jaccard(vivos[i].digest, vivos[j].digest);
    if (sim < CFG.divLimiar) pares.push({ a: vivos[i], b: vivos[j], sim: Number(sim.toFixed(3)) });
  }
  pares.sort((x, y) => x.sim - y.sim);
  const top = pares.slice(0, 6);
  emit('divergencias', { total: pares.length, pares: top.map((p) => ({ a: p.a.id, b: p.b.id, sim: p.sim })) });
  if (usarArbitro && top.length && !parar()) {
    emit('phase', { phase: 'ARBITER', code: '07' });
    const corpo = top.map((p) =>
      `PAR C${String(p.a.id).padStart(2, '0')} [${p.a.persona}] × C${String(p.b.id).padStart(2, '0')} [${p.b.persona}] (similaridade ${p.sim})\n` +
      `A: ${p.a.digest}\nB: ${p.b.digest}`).join('\n\n');
    const fr = frame({ src: 0xff, dst: 0xff, phase: PHASE.ARBITER, payload: `CONSULTA:\n${input}\n\nPARES DIVERGENTES:\n${corpo}` });
    emit('frame', meta(fr));
    try {
      const r = bill(await callClaude({
        system: SYS_ARBITER, user: wire(fr, strict), maxTokens: 900,
        model: CFG.arbiterModel || CFG.claudeModel, label: 'árbitro', onWarn: warn,
        onDelta: (t) => emit('delta', { id: -2, side: 'arbitro', t }),
      }));
      arbitro = r.text;
      emit('arbitro', { text: r.text, ms: r.ms, pares: top.length, model: r.model });
    } catch (e) { warn(`árbitro falhou: ${e.message}`); }
  }

  /* ── 05 REDUCE ── */
  emit('phase', { phase: 'REDUCE', code: '05' });
  const dossie = live.map((c) => c.erro
    ? `### C${String(c.id).padStart(2, '0')} [${c.persona}] — OFFLINE: ${c.erro}`
    : `### C${String(c.id).padStart(2, '0')} [${c.persona}] — ${c.titulo}` +
      (c.confianca != null ? ` (confiança ${c.confianca})` : '') +
      (c.refs?.length ? `\nFONTES: ${c.refs.map((r) => r.titulo).join(' | ')}` : '') +
      `\nXREF:\n${c.xref}\nINTERP:\n${c.interp}` +
      c.refinos.map((r) => `\nREFINO r${r.round}:\n${r.text}`).join('') +
      (c.achados?.length ? `\nVERIFICADOR: ${c.achados.map((a) => `[${a.sev.toUpperCase()} ${a.tipo}] ${a.nota} — "${a.trecho}"`).join(' | ')}` : '')).join('\n\n');
  const cruzadoTxt = cruzados.length
    ? `\n\nVERIFICADOR CRUZADO (determinístico, entre clusters):\n` +
      cruzados.map((a) => `[${a.sev.toUpperCase()} ${a.tipo}] C${a.clusters.map((i) => String(i).padStart(2, '0')).join(' × C')} — ${a.nota} — "${a.trecho}"`).join('\n')
    : '';
  const rf = frame({ src: 0xff, dst: 0xff, phase: PHASE.REDUCE, payload: `CONSULTA ORIGINAL:\n${input}\n\nPERFIL: ${montagem.nome}\n\nDOSSIÊ DOS CLUSTERS:\n${dossie}${cruzadoTxt}` + (arbitro ? `\n\nPARECER DO ÁRBITRO:\n${arbitro}` : '') });
  emit('frame', meta(rf));
  let reduce = '';
  try {
    const r = bill(await callClaude({
      system: SYS_REDUCE, user: wire(rf, strict), maxTokens: 2200, label: 'redutor', onWarn: warn,
      onDelta: (t) => emit('delta', { id: -1, side: 'reduce', t }),
    }));
    reduce = r.text;
    emit('reduce', { text: r.text, ms: r.ms });
  } catch (e) { emit('error', { message: `redutor falhou: ${e.message}` }); }

  const job = {
    id: jobId, criado: new Date().toISOString(), input,
    perfil: montagem.composto ? 'composto' : (opts.perfil || 'geral'), perfilNome: montagem.nome,
    lentes: personas.map((p, i) => ({ tag: p.tag, perfil: p.perfil || (opts.perfil || 'geral'), fonte: eixos[i]?.fonte || null })),
    clusters: n, strict, rounds: maxRounds, fonte, teto, estourou,
    models: { claude: CFG.claudeModel, gemini: CFG.geminiModel },
    convergencia, divergencias: top.map((p) => ({ a: p.a.id, b: p.b.id, sim: p.sim })), arbitro,
    verificacao: { achados: totalAchados, erros: totalErros, cruzados },
    clusters_dados: live, reduce, custo: acc, ms: Date.now() - t0,
  };
  try { await saveJob(job); } catch (e) { warn(`persistência: ${e.message}`); }
  emit('done', { id: jobId, ms: job.ms, calls: acc.calls, tin: acc.in, tout: acc.out, cost: acc.cost, offline: live.filter((c) => c.erro).length, convergencia, divergencias: pares.length, achados: totalAchados, erros: totalErros, teto, estourou });
}

/* -------------------------------------------------------- persistência */
async function saveJob(job) {
  await fsp.mkdir(CFG.jobsDir, { recursive: true });
  await fsp.writeFile(path.join(CFG.jobsDir, `${job.id}.json`), JSON.stringify(job, null, 1));
  const files = (await fsp.readdir(CFG.jobsDir)).filter((f) => f.endsWith('.json'));
  if (files.length > CFG.keepJobs) {
    const stats = await Promise.all(files.map(async (f) => ({ f, t: (await fsp.stat(path.join(CFG.jobsDir, f))).mtimeMs })));
    stats.sort((a, x) => a.t - x.t).slice(0, files.length - CFG.keepJobs)
      .forEach((s) => fsp.unlink(path.join(CFG.jobsDir, s.f)).catch(() => {}));
  }
}
async function listJobs() {
  try {
    const files = (await fsp.readdir(CFG.jobsDir)).filter((f) => f.endsWith('.json'));
    const rows = await Promise.all(files.map(async (f) => {
      try {
        const j = JSON.parse(await fsp.readFile(path.join(CFG.jobsDir, f), 'utf8'));
        return { id: j.id, criado: j.criado, clusters: j.clusters, perfil: j.perfilNome || j.perfil, fonte: j.fonte, custo: j.custo?.cost || 0, input: String(j.input).slice(0, 90) };
      } catch { return null; }
    }));
    return rows.filter(Boolean).sort((a, b) => b.criado.localeCompare(a.criado)).slice(0, 20);
  } catch { return []; }
}

/* ------------------------------------------------------------------ http */
const hits = new Map();
setInterval(() => { const now = Date.now(); for (const [k, v] of hits) if (!v.some((t) => now - t < CFG.rateWindowMs)) hits.delete(k); }, 300_000).unref();

function limited(ip) {
  const now = Date.now();
  const arr = (hits.get(ip) || []).filter((t) => now - t < CFG.rateWindowMs);
  arr.push(now); hits.set(ip, arr);
  return arr.length > CFG.rateMax;
}

/** comparação em tempo constante para não vazar o token por temporização */
function tokenOk(fornecido) {
  if (!CFG.accessToken) return true;
  const a = Buffer.from(String(fornecido || ''));
  const b = Buffer.from(CFG.accessToken);
  return a.length === b.length && crypto.timingSafeEqual(a, b);
}
const autorizado = (req, url) => tokenOk(req.headers['x-access-token'] || url.searchParams.get('t'));

const HEADERS_SEG = {
  'x-content-type-options': 'nosniff',
  'referrer-policy': 'no-referrer',
  'x-frame-options': 'DENY',
  'permissions-policy': 'geolocation=(), microphone=(), camera=()',
  'content-security-policy': [
    "default-src 'self'",
    "script-src 'self' 'unsafe-inline'",           // o app é um único HTML com script embutido
    "style-src 'self' 'unsafe-inline' https://fonts.googleapis.com",
    "font-src https://fonts.gstatic.com",
    "img-src 'self' data:",
    "connect-src 'self'",
    "base-uri 'none'", "form-action 'none'", "frame-ancestors 'none'",
  ].join('; '),
};

const MIME = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript; charset=utf-8', '.css': 'text/css; charset=utf-8', '.json': 'application/json; charset=utf-8', '.svg': 'image/svg+xml', '.webp': 'image/webp', '.png': 'image/png', '.ico': 'image/x-icon' };
function json(res, code, obj) {
  const s = JSON.stringify(obj);
  res.writeHead(code, { ...HEADERS_SEG, 'content-type': 'application/json; charset=utf-8', 'content-length': Buffer.byteLength(s) });
  res.end(s);
}
async function lerCorpo(req, limite) {
  let body = '';
  for await (const c of req) { body += c; if (body.length > limite) { req.destroy(); throw new Error('corpo excede o limite'); } }
  return body;
}

/* ---- corpus gerenciável pela interface (necessário para operar sem shell) ---- */
const nomeSeguro = (s) => String(s).normalize('NFD').replace(/[\u0300-\u036f]/g, '')
  .replace(/[^a-zA-Z0-9._-]/g, '-').replace(/-+/g, '-').replace(/^[.-]+/, '').slice(0, 60) || 'nota';

async function listarCorpus() {
  try {
    const files = (await fsp.readdir(CFG.corpusDir)).filter((f) => /\.(txt|md|json|csv|log)$/i.test(f));
    return Promise.all(files.map(async (f) => {
      const st = await fsp.stat(path.join(CFG.corpusDir, f));
      return { nome: f, bytes: st.size, mod: st.mtime.toISOString() };
    }));
  } catch { return []; }
}

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
  const ip = req.headers['x-forwarded-for']
    ? req.headers['x-forwarded-for'].split(',')[0].trim() : req.socket.remoteAddress;

  /* saúde é pública: responde antes do login e serve de sonda para o proxy */
  if (url.pathname === '/api/health') {
    return json(res, 200, {
      ok: true, versao: 4, protegido: !!CFG.accessToken,
      claude: !!CFG.anthropicKey, gemini: !!CFG.geminiKey,
      clusters: CFG.clusters, maxRounds: CFG.maxRounds,
      models: { claude: CFG.claudeModel, gemini: CFG.geminiModel, arbitro: CFG.arbiterModel || CFG.claudeModel },
      precos: { claude: priceOf(CFG.claudeModel), gemini: priceOf(CFG.geminiModel) },
      perfis: Object.entries(PROFILES).map(([k, p]) => ({ chave: k, nome: p.nome, desc: p.desc, personas: p.personas.map((x) => x.tag) })),
      catalogo: CATALOGO.map((l) => ({ chave: l.chave, perfil: l.perfil, perfilNome: l.perfilNome, tag: l.tag, aterrada: !!l.g })),
      tetoPadrao: CFG.maxCostUSD,
      fontes: Object.entries(SOURCES).map(([k, s]) => ({ chave: k, tag: s.tag, desc: s.desc, pronta: k !== 'http' || !!CFG.httpSource })),
    });
  }

  if (url.pathname.startsWith('/api/')) {
    if (!autorizado(req, url)) return json(res, 401, { error: 'token de acesso ausente ou inválido' });

    if (url.pathname === '/api/jobs') return json(res, 200, await listJobs());

    if (url.pathname.startsWith('/api/job/')) {
      const id = url.pathname.split('/').pop().replace(/[^A-F0-9]/gi, '');
      try { return json(res, 200, JSON.parse(await fsp.readFile(path.join(CFG.jobsDir, `${id}.json`), 'utf8'))); }
      catch { return json(res, 404, { error: 'execução não encontrada' }); }
    }

    if (url.pathname === '/api/decode' && req.method === 'POST') {
      try { return json(res, 200, parseFrame(JSON.parse(await lerCorpo(req, 500000)).hex)); }
      catch (e) { return json(res, 400, { error: e.message }); }
    }

    /* corpus pela interface — operar do tablet sem shell */
    if (url.pathname === '/api/corpus' && req.method === 'GET') {
      return json(res, 200, { dir: CFG.corpusDir, limite: CFG.maxCorpusBytes, arquivos: await listarCorpus() });
    }
    if (url.pathname === '/api/corpus' && req.method === 'POST') {
      let p;
      try { p = JSON.parse(await lerCorpo(req, CFG.maxCorpusBytes + 4096)); }
      catch (e) { return json(res, 413, { error: e.message }); }
      const conteudo = String(p.conteudo || '');
      if (!conteudo.trim()) return json(res, 400, { error: 'conteúdo vazio' });
      if (Buffer.byteLength(conteudo) > CFG.maxCorpusBytes) return json(res, 413, { error: `limite de ${CFG.maxCorpusBytes} bytes por arquivo` });
      const atuais = await listarCorpus();
      let nome = nomeSeguro(p.nome || `nota-${Date.now()}`);
      if (!/\.(txt|md|json|csv|log)$/i.test(nome)) nome += '.md';
      if (atuais.length >= CFG.maxCorpusFiles && !atuais.some((a) => a.nome === nome)) {
        return json(res, 409, { error: `limite de ${CFG.maxCorpusFiles} arquivos no corpus` });
      }
      await fsp.mkdir(CFG.corpusDir, { recursive: true });
      await fsp.writeFile(path.join(CFG.corpusDir, nome), conteudo);
      return json(res, 200, { ok: true, nome, arquivos: await listarCorpus() });
    }
    if (url.pathname.startsWith('/api/corpus/') && req.method === 'DELETE') {
      const nome = nomeSeguro(decodeURIComponent(url.pathname.split('/').pop()));
      try { await fsp.unlink(path.join(CFG.corpusDir, nome)); return json(res, 200, { ok: true, arquivos: await listarCorpus() }); }
      catch { return json(res, 404, { error: 'arquivo não encontrado' }); }
    }

    if (url.pathname === '/api/run' && req.method === 'POST') {
      if (limited(ip)) return json(res, 429, { error: 'Limite de execuções atingido. Aguarde um minuto.' });
      let p;
      try { p = JSON.parse(await lerCorpo(req, 200000)); }
      catch (e) { return json(res, 400, { error: `corpo inválido: ${e.message}` }); }
      const input = String(p.input || '').trim().slice(0, CFG.maxInput);
      if (!input) return json(res, 400, { error: 'Consulta vazia' });

      res.writeHead(200, { ...HEADERS_SEG, 'content-type': 'text/event-stream; charset=utf-8', 'cache-control': 'no-cache, no-transform', connection: 'keep-alive', 'x-accel-buffering': 'no' });
      let closed = false;
      req.on('close', () => { closed = true; });
      const emit = (ev, data) => { if (!closed && !res.writableEnded) res.write(`event: ${ev}\ndata: ${JSON.stringify(data)}\n\n`); };
      const beat = setInterval(() => !closed && !res.writableEnded && res.write(': ping\n\n'), 15000);
      const t0 = Date.now();
      try { await runJob(input, p, emit, () => closed); }
      catch (e) { emit('error', { message: e.message }); }
      finally {
        clearInterval(beat);
        if (!closed) res.end();
        console.log(`[run] ${ip} ${p.perfil || 'geral'} ${p.clusters || CFG.clusters}C ${((Date.now() - t0) / 1000).toFixed(1)}s${closed ? ' cancelado' : ''}`);
      }
      return;
    }

    return json(res, 404, { error: 'rota inexistente' });
  }

  /* estáticos */
  const raiz = path.join(__dirname, 'public');
  const file = url.pathname === '/' ? '/index.html' : url.pathname;
  const full = path.join(raiz, path.normalize(file).replace(/^(\.\.[/\\])+/, ''));
  if (!full.startsWith(raiz)) { res.writeHead(403, HEADERS_SEG); return res.end('403'); }
  fs.readFile(full, (err, data) => {
    if (err) { res.writeHead(404, { ...HEADERS_SEG, 'content-type': 'text/plain' }); return res.end('404'); }
    res.writeHead(200, { ...HEADERS_SEG, 'content-type': MIME[path.extname(full)] || 'application/octet-stream', 'cache-control': 'no-cache' });
    res.end(data);
  });
});

server.headersTimeout = 20000;
server.requestTimeout = 0;          // SSE: a resposta é longa por natureza
server.keepAliveTimeout = 65000;

const HOST = process.env.HOST || '0.0.0.0';
server.listen(CFG.port, HOST, () => {
  console.log(`UROBOROS v4 ${HOST}:${CFG.port} — ${CFG.clusters} clusters | ${CFG.claudeModel} × ${CFG.geminiModel}`);
  console.log(`  perfis: ${Object.keys(PROFILES).join(', ')} | fontes: ${Object.keys(SOURCES).join(', ')}`);
  if (!CFG.anthropicKey) console.warn('  ! ANTHROPIC_API_KEY ausente');
  if (!CFG.geminiKey) console.warn('  ! GEMINI_API_KEY ausente');
  if (!CFG.accessToken) console.warn('  ! ACCESS_TOKEN ausente: qualquer visitante gasta suas chaves de API');
});

/* desligamento gracioso: deixa as execuções em curso terminarem antes de sair */
let encerrando = false;
for (const sinal of ['SIGTERM', 'SIGINT']) {
  process.on(sinal, () => {
    if (encerrando) return process.exit(1);
    encerrando = true;
    console.log(`\n${sinal} recebido — encerrando sem cortar execuções em curso`);
    server.close(() => process.exit(0));
    setTimeout(() => process.exit(0), 30_000).unref();
  });
}
process.on('unhandledRejection', (e) => console.error('[promessa rejeitada]', e?.message || e));
