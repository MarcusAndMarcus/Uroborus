# UROBOROS v4

Supercomputador de N clusters. Cada cluster é um par **Claude (cruzamento) + Gemini
(interpretação)** sob uma **lente fixa**. Os clusters conversam por quadros hexadecimais
(`URB1`) num anel com cordas, reconciliam até estabilizar, passam por um verificador
determinístico, e as contradições que sobram vão para um árbitro.

**O sistema não é de nenhum assunto.** O domínio é definido pelo *perfil* — um conjunto de
doze lentes. Trocar o perfil troca todo o comportamento sem tocar em código.

Node 20+, **zero dependências npm**.

## Hospedagem real

**GitHub Pages não serve.** Pages entrega arquivo estático: não roda processo Node, não guarda
segredo e não mantém conexão SSE. O `index.html` até abriria, e falharia em toda chamada de API.

O GitHub serve como **origem**, não como servidor. O fluxo abaixo é o que funciona.

### Repositório + entrega contínua

```bash
git init && git add . && git commit -m "uroboros"
git remote add origin git@github.com:seu-usuario/uroboros.git
git push -u origin main
```

O `.gitignore` já exclui `.env`, `jobs/` e `corpus/` — dossiês e documentos ficam só no servidor.

Dois fluxos em `.github/workflows/`:

- **`ci.yml`** — a cada push: confere sintaxe do backend e do frontend, roda o `test-mock.js`
  inteiro (sem gastar token) e falha se algum padrão de chave de API aparecer no repositório.
- **`deploy.yml`** — só depois que a verificação passa: rsync para a VPS, reinicia o serviço e
  espera o `/api/health` responder; se não responder em 30 s, despeja o log e falha.

Segredos a criar em *Settings → Secrets and variables → Actions*: `SSH_HOST`, `SSH_USER`,
`SSH_KEY` (chave privada de deploy) e, se não for a 22, `SSH_PORT`. No servidor, o usuário de
deploy precisa de uma linha em `/etc/sudoers.d/uroboros`:

```
deploy ALL=(root) NOPASSWD: /usr/bin/systemctl restart uroboros, /usr/bin/chown -R uroboros\:uroboros /opt/uroboros
```

### VPS com TLS (recomendado)

```bash
scp -r uroboros/ root@servidor:/opt/
ssh root@servidor 'bash /opt/uroboros/deploy/instalar-vps.sh uroboros.seudominio.com.br'
```

Ou direto do repositório, sem scp:

```bash
ssh root@servidor 'REPO=https://github.com/seu-usuario/uroboros.git bash <(curl -fsSL https://raw.githubusercontent.com/seu-usuario/uroboros/main/deploy/instalar-vps.sh) uroboros.seudominio.com.br'
```

O script instala Node 22 e Caddy, cria o usuário de serviço, gera um `ACCESS_TOKEN`
aleatório em `/etc/uroboros.env`, sobe o systemd com sandbox e fecha o firewall.
Depois preencha as chaves de API nesse arquivo e `systemctl restart uroboros`.

Atualizações manuais, direto do tablet:

```bash
bash deploy/atualizar.sh root@servidor     # rsync + restart, preserva jobs e corpus
```

### Fly.io — construindo do repositório, sem VPS

```bash
flyctl launch --no-deploy --copy-config --config deploy/fly.toml
flyctl volumes create dados --size 1 --region gru
flyctl secrets set ANTHROPIC_API_KEY=... GEMINI_API_KEY=... ACCESS_TOKEN=...
flyctl deploy
```

O volume não é opcional: sem ele, `jobs/` e `corpus/` somem a cada implantação.
`auto_stop_machines = false` também não é opcional — uma máquina que dorme derruba
a execução no meio.

### Docker em qualquer lugar

```bash
cp .env.example .env    # preencha as chaves e o ACCESS_TOKEN
docker compose -f deploy/docker-compose.yml up -d --build
```

A porta é publicada só em `127.0.0.1`; quem expõe para a internet é o proxy.

### O que exige atenção

- **`ACCESS_TOKEN` não é opcional numa máquina pública.** Sem ele, qualquer um que
  encontre a URL executa jobs com as suas chaves. O servidor avisa no boot se estiver aberto.
- **SSE precisa de proxy que não bufferize.** O Caddy já serve com `flush_interval -1`.
  Em Nginx: `proxy_buffering off; proxy_read_timeout 600s;`. Sem isso a grade só aparece no fim.
- **Serverless não serve**: Netlify, Vercel e Cloudflare Workers cortam a resposta bem antes
  de uma execução de 10 clusters terminar, e não fazem streaming real.
- **Disco efêmero mata a persistência.** Render e Railway sem volume apagam `jobs/` a cada
  implantação. Confira se a plataforma escolhida oferece volume antes de decidir.
- Uma execução de 10 clusters leva minutos; `TimeoutStopSec=45` no systemd dá margem
  para o desligamento gracioso terminar o que estava em curso.

## Rodar local

```bash
export ANTHROPIC_API_KEY=sk-ant-... GEMINI_API_KEY=AIza...
node server.js            # http://127.0.0.1:8080
node test-mock.js         # bateria completa com APIs simuladas, sem gastar token
```

## Perfis e composição livre

Cada execução usa doze lentes de um perfil — **ou** uma seleção livre de lentes de perfis
diferentes. Marcar lentes em `COMPOR LENTES` ignora o perfil e faz o número de clusters ser
o número de lentes marcadas, na ordem em que foram marcadas.

```jsonc
// 4 lentes de 4 perfis distintos, com fonte própria em duas delas
{ "lentes": ["software:0", "seguranca:2", "negocio:2", "matematica:1"],
  "fontesLente": { "1": "corpus", "2": "web" } }
```

O catálogo completo, com a chave `perfil:índice` de cada lente, está em `/api/health`.

| chave | lentes |
|---|---|
| `geral` | cético · quantitativo · histórico · estrutural · adversarial · empírico · sistêmico · econômico · analógico · operacional · epistêmico · contrafactual |
| `software` | arquitetura · desempenho · confiabilidade · segurança · dados · contrato · testabilidade · operação · custo · dívida · experiência · migração |
| `seguranca` | superfície · ameaça · detecção · resposta · suprimentos · criptografia · identidade · auditoria · conformidade · resiliência · fator humano · recuperação |
| `ciencia` | hipótese · método · dados · estatística · reprodução · literatura · viés · mecanismo · escala · instrumento · alternativa · implicação |
| `matematica` | enunciado · casos pequenos · invariante · contraexemplo · generalização · estrutura · cotas · método · obstrução · análogo · rigor · computacional |
| `negocio` | problema · cliente · mercado · receita · custo · concorrência · regulatório · execução · risco · métrica · escala · continuidade |

Criar um perfil é acrescentar uma entrada em `PROFILES` no `server.js`. A marca `g:1` indica
quais lentes recebem evidência externa por padrão. O perfil `seguranca` é deliberadamente
defensivo — detecção, resposta, conformidade, recuperação — sem lente ofensiva.

## Camada de fontes

| fonte | o que faz |
|---|---|
| `nenhuma` | sem coleta; o sistema opera só com o que os modelos sabem |
| `web` | busca do Gemini com grounding; devolve texto e links de origem |
| `http` | GET em `SOURCE_HTTP`, com `{q}` substituído pela consulta |
| `corpus` | trechos de `.txt/.md/.json/.csv/.log` em `CORPUS_DIR`, ranqueados por sobreposição de termos |

`http` é o ponto de extensão e é agnóstico de assunto — catálogo científico, base de
vulnerabilidades, ERP da empresa, API própria:

```bash
SOURCE_HTTP='https://api.exemplo.com/busca?q={q}' node server.js
```

O corpus é gerenciável pela própria interface (gravar, listar, apagar), sem shell no servidor.

**Fonte por lente.** A fonte global vale para as lentes marcadas com `g`. `fontesLente` sobrepõe
essa escolha por posição de cluster: `CUSTO` pode buscar preço num endpoint `http`, `LITERATURA`
usar `web` e `DADOS` ler o `corpus`, tudo na mesma execução.

## Teto de gasto

`MAX_COST_USD` (padrão US$ 1,00) e o campo `TETO US$` na tela inicial limitam o custo por execução.
Ao encostar no teto o sistema para de iniciar trabalho novo, marca o custo em vermelho e vai
direto para a síntese com o que já existe — nada é perdido.

Chamadas já em voo terminam, então o gasto final pode passar um pouco do teto: com concorrência 4,
até quatro chamadas podem concluir depois do disparo. O rate limit protege contra volume de
execuções; o teto protege contra uma execução cara.

## Verificador determinístico

Roda entre o gossip e o árbitro, **sem LLM**, sobre o texto de cada cluster. Não julga mérito:
aponta o que é insustentável por conta própria.

| verificação | o que pega |
|---|---|
| aritmética | `120 * 3 = 380` — resultado declarado que não bate com o calculado |
| intervalo | `entre 90 e 30 minutos` — limite inferior maior que o superior |
| data | `31/02/2025` — data inexistente; e períodos que terminam antes de começar |
| unidade | `2 GB = 3000 MB` — conversão que não fecha, ou igualdade entre grandezas diferentes |
| percentual | participação acima de 100% sem indicar variação |
| citação | URL que não está entre as fontes coletadas — ou citada sem coleta nenhuma |
| coerência | confiança ≥ 0,9 declarada junto com `[LACUNA:]` do próprio cluster |

E duas verificações **entre** clusters, que nenhum cluster consegue fazer sozinho:

| verificação cruzada | o que pega |
|---|---|
| grandeza divergente | mesma grandeza, mesma unidade, valores com razão ≥ 3× entre clusters — `300 ms` num, `9 s` noutro |
| contradição | um cluster nega nos mesmos termos o que outro afirma (Jaccard ≥ 0,55 após remover a negação) |

Os achados vão para a interface **e para o dossiê do redutor**, com instrução explícita de
descartar ou corrigir o que estiver marcado como erro em vez de repetir. É o que impede o
sistema de sintetizar com elegância um número errado.

## Protocolo URB1

```
55524231 | VER(1B) | SRC(1B) | DST(1B) | FASE(1B) | TTL(1B) | LEN(2B) | PAYLOAD | CRC16-CCITT(2B)
```

`FF` = barramento, `FE` = camada de fontes. PAYLOAD é UTF-8 em hexadecimal.
CRC16-CCITT (poly `0x1021`, init `0xFFFF`) — mesma implementação do BR Code do Pix.
O decodificador aceita também o magic legado `PLX1` das versões anteriores.

Fases: `01` SCATTER · `02` XREF · `03` INTERP · `04` GOSSIP · `05` REDUCE · `06` EVIDENCE · `07` ARBITER

```bash
curl -s -X POST localhost:8080/api/decode -H 'content-type: application/json' \
  -H "x-access-token: $ACCESS_TOKEN" -d '{"hex":"55524231..."}'
```

## Pipeline

1. **SCATTER** — Claude decompõe a consulta em N eixos ortogonais, alinhados às lentes.
2. **EVIDENCE** *(opcional)* — a fonte coleta para as lentes aterradas.
3. **XREF** — o Claude de cada cluster cruza seu eixo sob a lente.
4. **INTERP** — o Gemini do par amplia, contesta e declara `CONFIANCA`.
5. **GOSSIP** — digests circulam entre vizinhos `(i±1, i+3)` e o Claude reconcilia.
   Da segunda rodada em diante, **só reconcilia quem está inseguro ou instável**.
6. **VERIFICAÇÃO** — auditoria determinística por cluster e entre clusters, sem custo de token.
7. **ARBITER** — pares com similaridade abaixo de `DIV_LIMIAR` são detectados
   deterministicamente e julgados: divergência real, aparente ou complementaridade.
8. **REDUCE** — síntese final, com verificação e parecer do árbitro no dossiê.

**Δ de convergência** = `1 − média(Jaccard(digest_anterior, digest_novo))` sobre os clusters
que reconciliaram. Aparece por rodada na topbar.

Chamadas com N=10, 1 rodada, sem fonte: `32`. Cada rodada extra custa no máximo +10,
e menos conforme os clusters estabilizam.

## Operação

- **Streaming real** token a token dos dois provedores, pintado em lote por `requestAnimationFrame`.
- **Retry** com backoff em 429/5xx/overloaded; só repete se nada foi emitido.
- **Cancelar**: `PARAR` aborta o fetch e o servidor não inicia novas chamadas.
- **Cluster offline** não derruba a execução; o REDUCE registra a ausência.
- **Custo** estimado ao vivo pela tabela `PRICES` — **confira os valores**, preços mudam.
- **Persistência**: cada execução vira `jobs/<ID>.json`, gravado antes do evento `done`.
  A tela inicial lista as últimas 20; clicar recarrega tudo sem gastar token.
- **Endurecimento HTTP**: CSP, `nosniff`, `frame-ancestors 'none'`, limites de corpo,
  comparação de token em tempo constante, travessia de caminho bloqueada nos estáticos.

## Atalhos

`Ctrl+Enter` inicia · `0`–`9` abre o cluster · `←` `→` navega · `S` síntese · `Esc` fecha.
