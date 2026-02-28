import os
import logging
import sqlite3
from datetime import datetime, timedelta
import google.generativeai as genai
import pandas as pd
import io
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    MessageHandler, filters, ContextTypes
)

BOT_TOKEN = os.environ["BOT_TOKEN"]
GEMINI_API_KEY = os.environ["GEMINI_API_KEY"]
ADMIN_ID = int(os.environ["ADMIN_ID"])

genai.configure(api_key=GEMINI_API_KEY)
model = genai.GenerativeModel("gemini-1.5-pro")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
DB = "/data/clients.db"

def init_db():
    os.makedirs("/data", exist_ok=True)
    with sqlite3.connect(DB) as conn:
        conn.execute("""CREATE TABLE IF NOT EXISTS clients (
            telegram_id INTEGER PRIMARY KEY,
            unique_code TEXT UNIQUE,
            language TEXT DEFAULT 'uz',
            is_active INTEGER DEFAULT 0,
            subscription_end TEXT,
            created_at TEXT)""")
        conn.commit()

def get_client(tid):
    with sqlite3.connect(DB) as conn:
        return conn.execute("SELECT * FROM clients WHERE telegram_id=?", (tid,)).fetchone()

def get_client_by_code(code):
    with sqlite3.connect(DB) as conn:
        return conn.execute("SELECT * FROM clients WHERE unique_code=?", (code,)).fetchone()

def create_client(tid, lang):
    with sqlite3.connect(DB) as conn:
        count = conn.execute("SELECT COUNT(*) FROM clients").fetchone()[0] + 1
        code = f"ZH-705{count:06d}"
        conn.execute("INSERT OR IGNORE INTO clients VALUES (?,?,?,0,?,?)",
                     (tid, code, lang, None, datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        conn.commit()
    return code

def set_language(tid, lang):
    with sqlite3.connect(DB) as conn:
        conn.execute("UPDATE clients SET language=? WHERE telegram_id=?", (lang, tid))
        conn.commit()

def activate(code, months=1):
    end = (datetime.now() + timedelta(days=30*months)).strftime("%Y-%m-%d")
    with sqlite3.connect(DB) as conn:
        conn.execute("UPDATE clients SET is_active=1, subscription_end=? WHERE unique_code=?", (end, code))
        conn.commit()

def deactivate(code):
    with sqlite3.connect(DB) as conn:
        conn.execute("UPDATE clients SET is_active=0, subscription_end=NULL WHERE unique_code=?", (code,))
        conn.commit()

def check_active(tid):
    c = get_client(tid)
    if not c or not c[3]: return False
    if c[4] and datetime.now() > datetime.strptime(c[4], "%Y-%m-%d"): return False
    return True

def get_lang(tid):
    c = get_client(tid)
    return c[2] if c else "uz"

def all_clients():
    with sqlite3.connect(DB) as conn:
        return conn.execute("SELECT * FROM clients").fetchall()

TX = {
    "uz": {
        "registered": "✅ Ro'yxatdan o'tdingiz!\n\n🆔 Raqamingiz: <b>{code}</b>\n\nBu raqamni saqlang — to'lovni shu raqam orqali tekshiramiz.",
        "not_active": "❌ Obunangiz faol emas.\n\n🆔 Raqamingiz: <b>{code}</b>\n\nTo'lov qilish uchun adminga murojaat qiling.",
        "choose": "📊 Qaysi bo'limni xohlaysiz?",
        "send_file": "📁 Excel yoki CSV fayl yuboring.",
        "analyzing": "⏳ Tahlil qilinmoqda...",
        "file_error": "❌ Fayl o'qilmadi. Excel (.xlsx) yoki CSV (.csv) yuboring.",
        "back": "⬅️ Orqaga",
        "sections": {"sotuv":"📊 Сотув бўлими","moliya":"💰 Молия бўлими","mijozlar":"👥 Мижозлар бўлими","tahlil":"📈 Таҳлил бўлими"},
        "questions": {
            "s1":"Энг кўп сотиладиган маҳсулот қайси?","s2":"Қайси ойда сотув юқори/паст бўлди?","s3":"Қайси мижоз энг кўп сотиб олди?",
            "m1":"Умумий фойда қанча?","m2":"Харажат қаерда кўп кетади?","m3":"Ўтган йил билан таққослаш",
            "j1":"Янги мижозлар сони?","j2":"Кетиб қолган мижозлар?","j3":"Содиқ мижозлар кимлар?",
            "t1":"Кейинги ойдаги сотув таҳлили","t2":"Қайси маҳсулотга инвестиция киритиш керак?","t3":"Тренд қандай?"}
    },
    "kz": {
        "registered": "✅ Тіркелдіңіз!\n\n🆔 Нөміріңіз: <b>{code}</b>\n\nБұл нөмірді сақтаңыз — төлемді осы нөмір арқылы тексереміз.",
        "not_active": "❌ Жазылымыңыз белсенді емес.\n\n🆔 Нөміріңіз: <b>{code}</b>\n\nТөлем жасау үшін әкімшіге хабарласыңыз.",
        "choose": "📊 Қандай бөлім керек?",
        "send_file": "📁 Excel немесе CSV файл жіберіңіз.",
        "analyzing": "⏳ Талдау жасалуда...",
        "file_error": "❌ Файл оқылмады. Excel (.xlsx) немесе CSV (.csv) жіберіңіз.",
        "back": "⬅️ Артқа",
        "sections": {"sotuv":"📊 Сатылым бөлімі","moliya":"💰 Қаржы бөлімі","mijozlar":"👥 Тұтынушылар бөлімі","tahlil":"📈 Талдау бөлімі"},
        "questions": {
            "s1":"Ең көп сатылатын тауар қайсы?","s2":"Қай айда сатылым жоғары/төмен болды?","s3":"Қай тұтынушы ең көп сатып алды?",
            "m1":"Жалпы пайда қанша?","m2":"Шығын қай жерде көп кетеді?","m3":"Өткен жылмен салыстыру",
            "j1":"Жаңа тұтынушылар саны?","j2":"Кетіп қалған тұтынушылар?","j3":"Ең адал тұтынушылар кімдер?",
            "t1":"Келесі айдағы сатылым талдауы","t2":"Қай өнімге инвестиция салу керек?","t3":"Тренд қандай?"}
    },
    "ru": {
        "registered": "✅ Вы зарегистрированы!\n\n🆔 Ваш номер: <b>{code}</b>\n\nСохраните этот номер — оплата проверяется по нему.",
        "not_active": "❌ Подписка неактивна.\n\n🆔 Ваш номер: <b>{code}</b>\n\nОбратитесь к администратору для оплаты.",
        "choose": "📊 Какой раздел вас интересует?",
        "send_file": "📁 Отправьте файл Excel или CSV.",
        "analyzing": "⏳ Анализируем...",
        "file_error": "❌ Файл не прочитан. Отправьте Excel (.xlsx) или CSV (.csv).",
        "back": "⬅️ Назад",
        "sections": {"sotuv":"📊 Продажи","moliya":"💰 Финансы","mijozlar":"👥 Клиенты","tahlil":"📈 Аналитика"},
        "questions": {
            "s1":"Какой товар продаётся больше всего?","s2":"В каком месяце продажи были выше/ниже?","s3":"Какой клиент купил больше всего?",
            "m1":"Какова общая прибыль?","m2":"Где больше всего расходов?","m3":"Сравнение с прошлым годом",
            "j1":"Количество новых клиентов?","j2":"Ушедшие клиенты?","j3":"Кто самые лояльные клиенты?",
            "t1":"Прогноз продаж на следующий месяц","t2":"В какой продукт стоит инвестировать?","t3":"Какой тренд?"}
    },
    "en": {
        "registered": "✅ You are registered!\n\n🆔 Your code: <b>{code}</b>\n\nSave this code — payments are verified by it.",
        "not_active": "❌ Subscription not active.\n\n🆔 Your code: <b>{code}</b>\n\nContact admin to make payment.",
        "choose": "📊 Which section do you need?",
        "send_file": "📁 Please send an Excel or CSV file.",
        "analyzing": "⏳ Analyzing...",
        "file_error": "❌ Could not read file. Send Excel (.xlsx) or CSV (.csv).",
        "back": "⬅️ Back",
        "sections": {"sotuv":"📊 Sales","moliya":"💰 Finance","mijozlar":"👥 Customers","tahlil":"📈 Analytics"},
        "questions": {
            "s1":"Which product sells the most?","s2":"Which month had highest/lowest sales?","s3":"Which customer bought the most?",
            "m1":"What is the total profit?","m2":"Where are the most expenses?","m3":"Comparison with last year",
            "j1":"Number of new customers?","j2":"Lost customers?","j3":"Who are the most loyal customers?",
            "t1":"Next month sales forecast","t2":"Which product to invest in?","t3":"What is the trend?"}
    }
}

SECTION_KEYS = {"sotuv":["s1","s2","s3"],"moliya":["m1","m2","m3"],"mijozlar":["j1","j2","j3"],"tahlil":["t1","t2","t3"]}
LANG_NAMES = {"uz":"o'zbek","kz":"qozoq","ru":"rus","en":"english"}

def t(lang, key, **kw):
    val = TX.get(lang, TX["uz"]).get(key, key)
    return val.format(**kw) if kw else val

def lang_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🇺🇿 Ўзбекча", callback_data="lang_uz"),
         InlineKeyboardButton("🇰🇿 Қазақша", callback_data="lang_kz")],
        [InlineKeyboardButton("🇷🇺 Русский", callback_data="lang_ru"),
         InlineKeyboardButton("🇬🇧 English", callback_data="lang_en")],
    ])

def main_kb(lang):
    s = TX[lang]["sections"]
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(s["sotuv"], callback_data="sec_sotuv")],
        [InlineKeyboardButton(s["moliya"], callback_data="sec_moliya")],
        [InlineKeyboardButton(s["mijozlar"], callback_data="sec_mijozlar")],
        [InlineKeyboardButton(s["tahlil"], callback_data="sec_tahlil")],
    ])

def questions_kb(lang, section):
    qs = TX[lang]["questions"]
    keys = SECTION_KEYS[section]
    btns = [[InlineKeyboardButton(qs[k], callback_data=f"q_{k}")] for k in keys]
    btns.append([InlineKeyboardButton(t(lang, "back"), callback_data="back")])
    return InlineKeyboardMarkup(btns)

async def show_menu(update_or_msg, lang, is_message=True):
    """Send main menu — works after activation too"""
    kb = main_kb(lang)
    text = t(lang, "choose")
    if is_message:
        await update_or_msg.reply_text(text, reply_markup=kb)
    else:
        await update_or_msg.message.reply_text(text, reply_markup=kb)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    tid = update.effective_user.id
    client = get_client(tid)
    if not client:
        await update.message.reply_text(
            "Assalomu alaykum / Сәлеметсіз бе / Здравствуйте / Hello! 👋\n\nTilni tanlang / Тілді таңдаңыз / Выберите язык / Choose language:",
            reply_markup=lang_kb()
        )
        return
    lang = client[2]
    if check_active(tid):
        await update.message.reply_text(t(lang, "choose"), reply_markup=main_kb(lang))
    else:
        await update.message.reply_text(t(lang, "not_active", code=client[1]), parse_mode="HTML")

async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    tid = q.from_user.id
    data = q.data

    if data.startswith("lang_"):
        lang = data[5:]
        client = get_client(tid)
        if not client:
            code = create_client(tid, lang)
            await q.edit_message_text(t(lang, "registered", code=code), parse_mode="HTML")
            await q.message.reply_text(t(lang, "not_active", code=code), parse_mode="HTML")
        else:
            set_language(tid, lang)
            if check_active(tid):
                await q.edit_message_text(t(lang, "choose"), reply_markup=main_kb(lang))
            else:
                await q.edit_message_text(t(lang, "not_active", code=client[1]), parse_mode="HTML")
        return

    lang = get_lang(tid)

    if data == "back":
        await q.edit_message_text(t(lang, "choose"), reply_markup=main_kb(lang))
        return

    if data.startswith("sec_"):
        section = data[4:]
        await q.edit_message_text(t(lang, "choose"), reply_markup=questions_kb(lang, section))
        return

    if data.startswith("q_"):
        if not check_active(tid):
            client = get_client(tid)
            code = client[1] if client else "—"
            await q.edit_message_text(t(lang, "not_active", code=code), parse_mode="HTML")
            return
        qkey = data[2:]
        qtext = TX[lang]["questions"].get(qkey, qkey)
        context.user_data["question"] = qtext
        await q.edit_message_text(f"❓ <b>{qtext}</b>\n\n{t(lang, 'send_file')}", parse_mode="HTML")
        return

async def on_file(update: Update, context: ContextTypes.DEFAULT_TYPE):
    tid = update.effective_user.id
    lang = get_lang(tid)

    if not check_active(tid):
        client = get_client(tid)
        code = client[1] if client else "—"
        await update.message.reply_text(t(lang, "not_active", code=code), parse_mode="HTML")
        return

    question = context.user_data.get("question")
    if not question:
        await update.message.reply_text(t(lang, "choose"), reply_markup=main_kb(lang))
        return

    doc = update.message.document
    if not doc:
        await update.message.reply_text(t(lang, "file_error"))
        return

    fname = doc.file_name or ""
    if not (fname.endswith(".xlsx") or fname.endswith(".xls") or fname.endswith(".csv")):
        await update.message.reply_text(t(lang, "file_error"))
        return

    wait = await update.message.reply_text(t(lang, "analyzing"))

    try:
        file = await doc.get_file()
        raw = await file.download_as_bytearray()
        fio = io.BytesIO(bytes(raw))
        df = pd.read_csv(fio) if fname.endswith(".csv") else pd.read_excel(fio)

        summary = f"Ustunlar: {list(df.columns)}\nQatorlar: {len(df)}\n{df.head(20).to_string()}\n{df.describe().to_string()}"
        prompt = f"Sen professional data analitiksan.\nSavol: {question}\nMa'lumotlar:\n{summary}\nJavobni {LANG_NAMES.get(lang,'o`zbek')} tilida yoz. Aniq raqamlar va qisqa tavsiyalar ber."

        response = model.generate_content(prompt)
        result = response.text

        await wait.delete()
        await update.message.reply_text(f"📊 <b>{question}</b>\n\n{result}", parse_mode="HTML")
        # Show menu again after analysis
        await update.message.reply_text(t(lang, "choose"), reply_markup=main_kb(lang))
        context.user_data.pop("question", None)

    except Exception as e:
        logger.error(f"File error: {e}")
        await wait.delete()
        await update.message.reply_text(t(lang, "file_error"))

async def cmd_activate(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    args = context.args
    if not args:
        await update.message.reply_text("Ishlatish: /activate ZH-705000001 yoki /activate ZH-705000001 3")
        return
    code = args[0]
    months = int(args[1]) if len(args) > 1 else 1
    client = get_client_by_code(code)
    if not client:
        await update.message.reply_text(f"❌ {code} topilmadi!")
        return
    activate(code, months)
    await update.message.reply_text(f"✅ {code} — {months} oyga faollashtirildi!")
    lang = client[2]
    notify = {
        "uz": f"✅ Obunangiz faollashtirildi ({months} oy)!",
        "kz": f"✅ Жазылымыңыз белсендірілді ({months} ай)!",
        "ru": f"✅ Подписка активирована ({months} мес.)!",
        "en": f"✅ Subscription activated ({months} month(s))!",
    }
    try:
        await context.bot.send_message(chat_id=client[0], text=notify.get(lang, notify["uz"]))
        # Send menu directly to client
        await context.bot.send_message(chat_id=client[0], text=t(lang, "choose"), reply_markup=main_kb(lang))
    except Exception: pass

async def cmd_deactivate(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    args = context.args
    if not args:
        await update.message.reply_text("Ishlatish: /deactivate ZH-705000001")
        return
    deactivate(args[0])
    await update.message.reply_text(f"❌ {args[0]} — o'chirildi!")

async def cmd_clients(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    clients = all_clients()
    if not clients:
        await update.message.reply_text("Mijozlar yo'q.")
        return
    text = "👥 <b>Barcha mijozlar:</b>\n\n"
    for c in clients:
        status = "✅" if c[3] else "❌"
        text += f"{status} <b>{c[1]}</b> | {c[2].upper()} | {c[4] or '—'}\n"
    await update.message.reply_text(text, parse_mode="HTML")

async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    await update.message.reply_text("""👨‍💼 <b>Admin buyruqlari:</b>

/activate ZH-705000001 — 1 oyga faollashtirish
/activate ZH-705000001 3 — 3 oyga faollashtirish
/deactivate ZH-705000001 — O'chirish
/clients — Barcha mijozlar
/adminhelp — Yordam""", parse_mode="HTML")

def main():
    init_db()
    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("activate", cmd_activate))
    app.add_handler(CommandHandler("deactivate", cmd_deactivate))
    app.add_handler(CommandHandler("clients", cmd_clients))
    app.add_handler(CommandHandler("adminhelp", cmd_help))
    app.add_handler(CallbackQueryHandler(on_callback))
    app.add_handler(MessageHandler(filters.Document.ALL, on_file))
    logger.info("Bot started!")
    app.run_polling()

if __name__ == "__main__":
    main()
