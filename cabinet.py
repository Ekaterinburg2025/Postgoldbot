"""
cabinet.py — 📱 «Рекламный кабинет» бота МП (Telegram Mini App).

Отдельный продукт для рекламодателей: города и цены, объявление с предпросмотром, оплата,
посты, автопосты, шаблоны, продление со скидкой.

Безопасность:
  • каждый запрос подписан Telegram (initData, HMAC от токена бота) — без подписи ответ 401;
  • цена считается ТОЛЬКО на сервере (mp.calc_ad_price / mp.create_offer), из браузера приходит лишь выбор;
  • публикация проходит те же проверки, что и в чате (mp.precheck_core: бан, доступ, стоп-слова, ссылки).

Вся бизнес-логика живёт в mpserv.py; здесь только «окно» к ней.
"""
import hashlib
import hmac
import json
import os
import threading
import time
from types import SimpleNamespace
from urllib.parse import parse_qsl

from bson.objectid import ObjectId
from flask import Response, jsonify, request, send_file

HERE = os.path.dirname(os.path.abspath(__file__))
HTML_PATH = os.path.join(HERE, "webapp", "cabinet.html")
INIT_MAX_AGE = 24 * 3600          # подпись initData действительна сутки
MAX_UPLOAD = 45 * 1024 * 1024     # Telegram принимает от бота до 50 МБ


def register_cabinet(app, mp):
    bot = mp.bot
    db = mp.db
    token = os.getenv("BOT_TOKEN", "")
    uploads = db['mp_uploads']    # что человек загрузил через кабинет (для превью и проверки владельца)
    try:
        uploads.create_index("dt", expireAfterSeconds=60 * 86400)
        uploads.create_index([("uid", 1), ("file_id", 1)])
    except Exception:
        pass
    app.config.setdefault("MAX_CONTENT_LENGTH", MAX_UPLOAD + 1024 * 1024)

    _rate = {}
    _rate_lock = threading.Lock()

    def throttle(key, limit, window):
        """Не больше limit действий за window секунд (в памяти процесса)."""
        now = time.time()
        with _rate_lock:
            arr = [t for t in _rate.get(key, []) if now - t < window]
            if len(arr) >= limit:
                _rate[key] = arr
                return False
            arr.append(now)
            _rate[key] = arr
            return True

    # ---------- ПОДПИСЬ TELEGRAM ----------
    def verify_init_data(init_data):
        if not init_data or not token:
            return None
        try:
            pairs = dict(parse_qsl(init_data, keep_blank_values=True, strict_parsing=False))
        except Exception:
            return None
        got_hash = pairs.pop("hash", "")
        if not got_hash:
            return None
        check = "\n".join(f"{k}={v}" for k, v in sorted(pairs.items()))
        secret = hmac.new(b"WebAppData", token.encode(), hashlib.sha256).digest()
        expected = hmac.new(secret, check.encode(), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(expected, got_hash):
            return None
        try:
            if time.time() - int(pairs.get("auth_date", "0")) > INIT_MAX_AGE:
                return None
            user = json.loads(pairs.get("user", "{}"))
        except Exception:
            return None
        if not user.get("id"):
            return None
        return user

    def auth():
        user = verify_init_data(request.headers.get("X-Init-Data", ""))
        if not user:
            return None
        return SimpleNamespace(id=int(user["id"]), first_name=user.get("first_name") or "", last_name=user.get("last_name") or "",
                               username=user.get("username") or "")

    def body():
        return request.get_json(silent=True) or {}

    def fail(msg, code=400, **extra):
        return jsonify({"ok": False, "error": msg, **extra}), code

    def unauthorized():
        return fail("Откройте кабинет из Telegram заново.", 401)

    # ---------- ВСПОМОГАТЕЛЬНОЕ ----------
    def fmt_dt(dt):
        dt = mp._aware(dt)
        try:
            return dt.astimezone(mp.ekb_tz).strftime("%d.%m.%Y %H:%M")
        except Exception:
            return "—"

    def resolve(net, city_raw):
        net = str(net or "")
        if net not in mp.NET_NAMES:
            return None, None
        city = mp.resolve_city(city_raw)
        if not city:
            return None, None
        if net != "all" and net not in mp.city_net_keys(city):
            return None, None
        if net == "all" and len(mp.city_net_keys(city)) < 2:
            return None, None
        return net, city

    def access_until(uid, net, city):
        nets = ["Все сети"] + ([] if net == "all" else [mp.NET_NAMES[net]])
        if net == "all":
            nets = ["Все сети"] + mp.ALL_NETWORKS  # для «Все сети» достаточно любой оплаченной
        sub = mp.ad_subs_collection.find_one({"user_id": uid, "city": city, "network": {"$in": nets},
                                              "end_date": {"$gt": mp.now_ekb()}}, sort=[("end_date", -1)])
        return sub

    def sign_media(uid, file_id, ttl=6 * 3600):
        exp = int(time.time()) + ttl
        sig = hmac.new(token.encode(), f"{uid}:{file_id}:{exp}".encode(), hashlib.sha256).hexdigest()[:32]
        return f"/cabinet/media?u={uid}&f={file_id}&e={exp}&s={sig}"

    def media_out(uid, media):
        out = []
        for m in media or []:
            item = {"type": m.get("type"), "id": m.get("id")}
            if m.get("type") == "photo":
                item["thumb"] = sign_media(uid, m["id"])
            out.append(item)
        return out

    def owns_media(uid, media):
        """Медиа в публикации — только свои: загруженные через кабинет или из своих шаблонов/черновика."""
        clean = []
        for m in (media or [])[:10]:
            t, fid = str(m.get("type", "")), str(m.get("id", ""))
            if t not in ("photo", "video") or not fid or len(fid) > 300:
                return None
            if not (uploads.find_one({"uid": uid, "file_id": fid}) or
                    mp.ad_templates_collection.find_one({"user_id": uid, "media_array.id": fid}) or
                    db['users'].find_one({"_id": uid, "temp_ad_media.id": fid})):
                return None
            clean.append({"type": t, "id": fid})
        return clean

    def balances(uid):
        pu = db['paid_users'].find_one({"uid": uid}, {"cashback_balance": 1, "bounty_points": 1}) or {}
        return {"rub": int(pu.get("cashback_balance", 0) or 0), "points": int(pu.get("bounty_points", 0) or 0)}

    def subs_view(uid):
        now = mp.now_ekb()
        grace = int(mp.net_cfg("mp_renew_grace_days"))
        since = now - mp.timedelta(days=max(grace, 1))
        rows = list(mp.ad_subs_collection.find({"user_id": uid, "end_date": {"$gte": since}}).sort("end_date", -1))
        stats = mp.get_user_statistics(uid)
        limit = int(mp.net_cfg("mp_daily_limit"))
        seen, out = set(), []
        for s in rows:
            key = (s.get("network"), s.get("city"))
            if key in seen:
                continue  # показываем последний (самый длинный) доступ на пару сеть+город
            seen.add(key)
            end = mp._aware(s["end_date"])
            net_key = mp.NET_KEYS.get(s.get("network"), "mk")
            left = (end - now).total_seconds()
            purchased = mp._aware(s.get("purchase_date")) if s.get("purchase_date") else None
            total = (end - purchased).total_seconds() if purchased else None
            used_today = 0
            if s.get("network") == "Все сети":
                for n in mp.ALL_NETWORKS:
                    used_today = max(used_today, stats["details"].get(n, {}).get(s.get("city"), {}).get("published", 0))
            else:
                used_today = stats["details"].get(s.get("network"), {}).get(s.get("city"), {}).get("published", 0)
            ren = mp.renewal_info(uid, net_key, s.get("city"))
            out.append({
                "id": str(s["_id"]), "network": s.get("network"), "net": net_key, "city": s.get("city"),
                "city_token": mp.city_token(s.get("city")), "until": fmt_dt(end), "seconds_left": int(left),
                "active": left > 0, "progress": max(0.0, min(1.0, left / total)) if total and total > 0 else None,
                "pin": bool(s.get("has_pin")), "links": bool(s.get("can_post_links")),
                "today": used_today, "limit": limit,
                "renew_pct": ren["pct"] if ren else 0,
                "renew_until": fmt_dt(ren["grace_until"]) if ren and not ren["active"] else None,
            })
        out.sort(key=lambda x: (not x["active"], x["seconds_left"] if x["active"] else -x["seconds_left"]))
        return out

    # ---------- СТРАНИЦА ----------
    @app.route('/cabinet')
    def cabinet_page():
        resp = send_file(HTML_PATH, mimetype="text/html")
        resp.headers["Cache-Control"] = "no-cache"
        return resp

    @app.route('/cabinet/media')
    def cabinet_media():
        """Превью своих фото по подписанной ссылке (img src не умеет слать заголовки)."""
        uid, fid, exp, sig = request.args.get("u", ""), request.args.get("f", ""), request.args.get("e", "0"), request.args.get("s", "")
        good = hmac.new(token.encode(), f"{uid}:{fid}:{exp}".encode(), hashlib.sha256).hexdigest()[:32]
        if not hmac.compare_digest(good, sig) or int(exp or 0) < time.time():
            return Response(status=403)
        try:
            info = bot.get_file(fid)
            data = bot.download_file(info.file_path)
        except Exception:
            return Response(status=404)
        resp = Response(data, mimetype="image/jpeg")
        resp.headers["Cache-Control"] = "private, max-age=21600"
        return resp

    # ---------- API ----------
    @app.route('/cabinet/api/bootstrap', methods=['POST'])
    def api_bootstrap():
        u = auth()
        if not u: return unauthorized()
        mp.refresh_matrix()
        cities = []
        for name in sorted(mp.all_cities.keys()):
            nets = mp.city_net_keys(name)
            if nets:
                cities.append({"name": name, "token": mp.city_token(name), "nets": nets,
                               "chats": sum(len(mp.all_cities[name][k]) for k in nets)})
        udoc = db['users'].find_one({"_id": u.id}, {"temp_ad_type": 1}) or {}
        return jsonify({
            "ok": True,
            "user": {"id": u.id, "first_name": u.first_name, "last_name": u.last_name, "username": u.username},
            "banned": mp.is_banned_in_network(u.id),
            "channel_approved": udoc.get("temp_ad_type") == "vip",
            "balances": balances(u.id),
            "networks": [{"key": k, "name": v} for k, v in mp.NET_NAMES.items() if k != "all"],
            "cities": cities,
            "subs": subs_view(u.id),
            "config": {
                "rub_per_star": mp._rub_per_star(), "points_per_star": mp._points_per_star(),
                "daily_limit": int(mp.net_cfg("mp_daily_limit")), "pin_markup": float(mp.net_cfg("mp_pin_markup")),
                "vip_markup": float(mp.net_cfg("mp_vip_markup")), "renew_pct": int(mp.net_cfg("mp_renew_discount")),
                "renew_grace_days": int(mp.net_cfg("mp_renew_grace_days")), "days": list(mp.VALID_DAYS),
                "multi_net": {"3": 10, "4": 20, "5": 30},
            },
            "signatures": mp.network_signatures,
            "support_url": mp.support_url(),
            "cheap_stars_url": str(mp.net_cfg("cheap_stars_url") or ""),
        })

    @app.route('/cabinet/api/subs', methods=['POST'])
    def api_subs():
        u = auth()
        if not u: return unauthorized()
        return jsonify({"ok": True, "subs": subs_view(u.id), "balances": balances(u.id)})

    @app.route('/cabinet/api/quote', methods=['POST'])
    def api_quote():
        u = auth()
        if not u: return unauthorized()
        d = body()
        net, city = resolve(d.get("net"), d.get("city"))
        if not net:
            return fail("Город или сеть недоступны.")
        promo, promo_state = None, None
        code = str(d.get("promo") or "").strip()
        if code:
            promo = mp.get_valid_promo(code, u.id)
            promo_state = "ok" if promo else "bad"
        tariffs = []
        for days in mp.VALID_DAYS:
            q = mp.calc_ad_price(u.id, days, net, city, False, promo)
            if not q:
                continue
            qp = mp.calc_ad_price(u.id, days, net, city, True, promo)
            tariffs.append({"days": days, "amount": q["amount"], "amount_pin": qp["amount"], "base": q["base"],
                            "before_promo": q["before_promo"]})
        if not tariffs:
            return fail("Для этого города сейчас нет тарифов.")
        q0 = mp.calc_ad_price(u.id, tariffs[0]["days"], net, city, False, promo)
        sub = access_until(u.id, net, city)
        ren = mp.renewal_info(u.id, net, city)
        return jsonify({"ok": True, "net": net, "city": city, "tariffs": tariffs,
                        "breakdown": {"vip_markup": q0["multiplier"], "buying_now": q0["buying_now"],
                                      "discount_net": q0["discount_net"], "renew_discount": q0["renew_discount"],
                                      "promo_value": q0["promo_value"], "promo_type": q0["promo_type"],
                                      "pin_markup": float(mp.net_cfg("mp_pin_markup"))},
                        "promo": promo_state, "promo_code": promo["_id"] if promo else None,
                        "access_until": fmt_dt(sub["end_date"]) if sub else None,
                        "renew_until": fmt_dt(ren["grace_until"]) if ren and not ren["active"] else None,
                        "balances": balances(u.id),
                        "rates": {"rub": mp._rub_per_star(), "points": mp._points_per_star()}})

    @app.route('/cabinet/api/checkout', methods=['POST'])
    def api_checkout():
        u = auth()
        if not u: return unauthorized()
        if not throttle(("checkout", u.id), 12, 60):
            return fail("Слишком часто. Подождите минуту.", 429)
        if mp.is_banned_in_network(u.id):
            return fail(mp.BANNED_TEXT.replace("\n", " "), 403)
        d = body()
        net, city = resolve(d.get("net"), d.get("city"))
        try:
            days = int(d.get("days"))
        except (TypeError, ValueError):
            days = 0
        method = str(d.get("method") or "")
        if not net or days not in mp.VALID_DAYS or method not in ("stars", "usdt", "ton", "rub", "points", "card"):
            return fail("Проверьте выбор тарифа.")
        code = str(d.get("promo") or "").strip()
        promo = mp.get_valid_promo(code, u.id) if code else None
        if code and not promo:
            return fail("Промокод больше недействителен — уберите его или введите другой.")
        o = mp.create_offer(u.id, days, net, city, bool(d.get("pin")), promo)
        if not o:
            return fail("Этот тариф сейчас недоступен.")
        amount, desc, payload, oid = o["amount"], o["description"], o["payload"], str(o["offer_id"])

        if method == "stars":
            try:
                link = bot.create_invoice_link(
                    title=("Доступ + ЗАКРЕП 📌" if d.get("pin") else "Доступ к публикации 📢"),
                    description=desc, payload=payload, provider_token="", currency="XTR",
                    prices=[mp.types.LabeledPrice(label="Рекламный доступ", amount=amount)])
            except Exception as e:
                mp.log_error("кабинет: счёт звёздами", e, u.id)
                return fail("Не удалось выставить счёт. Попробуйте ещё раз.", 502)
            return jsonify({"ok": True, "kind": "invoice", "link": link, "amount": amount, "offer_id": oid})

        if method in ("usdt", "ton"):
            url = mp.get_crypto_pay_url(f"{payload}___{u.id}", amount, f"Реклама: {o['network']} ({city})", asset=method.upper())
            if not url:
                return fail("CryptoBot сейчас недоступен. Выберите другой способ.", 502)
            return jsonify({"ok": True, "kind": "link", "link": url, "amount": amount, "rub": int(amount * mp._rub_per_star()), "offer_id": oid})

        if method in ("rub", "points"):
            ok_, text = mp.pay_offer_from_wallet(oid, u.id, method == "points")
            if not ok_:
                return fail(text)
            return jsonify({"ok": True, "kind": "paid", "message": text, "subs": subs_view(u.id), "balances": balances(u.id)})

        # card: альтернативная оплата через поддержку
        rub = int(amount * mp._rub_per_star())
        return jsonify({"ok": True, "kind": "card", "amount": amount, "rub": rub, "offer_id": oid,
                        "support_url": mp.support_url(),
                        "text": f"Оплата картой / СБП: {amount}⭐️ × {mp._rub_per_star():g} ₽ = {rub} ₽. "
                                f"Напишите в поддержку и укажите номер счёта {oid} — администратор пришлёт реквизиты и выдаст доступ."})

    @app.route('/cabinet/api/check_text', methods=['POST'])
    def api_check_text():
        u = auth()
        if not u: return unauthorized()
        d = body()
        text = str(d.get("text") or "")[:4000]
        net, city = resolve(d.get("net"), d.get("city"))
        can_links = False
        if net:
            sub = access_until(u.id, net, city)
            can_links = bool(sub and sub.get("can_post_links"))
        bad, word = mp.check_stop_words(text, ignore_black_zone=can_links)
        if bad:
            return jsonify({"ok": True, "valid": False, "reason": f"Запрещённое слово: «{word}»"})
        if not can_links and mp.has_links(text):
            return jsonify({"ok": True, "valid": False, "reason": "Ссылки и @username запрещены"})
        if len(text) > 3500:
            return jsonify({"ok": True, "valid": False, "reason": "Слишком длинный текст (максимум 3500)"})
        return jsonify({"ok": True, "valid": True})

    @app.route('/cabinet/api/media', methods=['POST'])
    def api_media():
        u = auth()
        if not u: return unauthorized()
        if not throttle(("media", u.id), 30, 600):
            return fail("Слишком много загрузок. Подождите несколько минут.", 429)
        f = request.files.get("file")
        if not f:
            return fail("Файл не получен.")
        mime = (f.mimetype or "").lower()
        if mime.startswith("image/"):
            kind = "photo"
        elif mime.startswith("video/"):
            kind = "video"
        else:
            return fail("Можно загрузить только фото или видео.")
        data = f.read()
        if len(data) > MAX_UPLOAD:
            return fail("Файл больше 45 МБ.")
        if kind == "photo" and len(data) > 10 * 1024 * 1024:
            return fail("Фото больше 10 МБ.")
        # Отправляем файл человеку в личку беззвучно, берём file_id и сразу удаляем сообщение
        try:
            import io
            from telebot.types import InputFile
            if kind == "photo":
                msg = bot.send_photo(u.id, InputFile(io.BytesIO(data), file_name=f.filename or "photo.jpg"), disable_notification=True)
                fid = msg.photo[-1].file_id
            else:
                msg = bot.send_video(u.id, InputFile(io.BytesIO(data), file_name=f.filename or "video.mp4"), disable_notification=True, supports_streaming=True)
                fid = msg.video.file_id
            try: bot.delete_message(u.id, msg.message_id)
            except Exception: pass
        except Exception as e:
            mp.log_error("кабинет: загрузка медиа", e, u.id)
            return fail("Telegram не принял файл. Проверьте, что бот не заблокирован, и попробуйте ещё раз.", 502)
        uploads.insert_one({"uid": u.id, "file_id": fid, "type": kind, "dt": mp.datetime.utcnow()})
        return jsonify({"ok": True, "media": media_out(u.id, [{"type": kind, "id": fid}])[0]})

    _publishing = set()

    @app.route('/cabinet/api/publish', methods=['POST'])
    def api_publish():
        u = auth()
        if not u: return unauthorized()
        d = body()
        net, city = resolve(d.get("net"), d.get("city"))
        if not net:
            return fail("Город или сеть недоступны.")
        selected_network = mp.NET_NAMES[net]
        text = str(d.get("text") or "")
        media = owns_media(u.id, d.get("media"))
        if media is None:
            return fail("Медиафайлы не найдены — загрузите их заново.")
        mode = d.get("mode", "once")
        interval = 0
        if mode == "auto":
            try:
                interval = int(d.get("interval"))
            except (TypeError, ValueError):
                interval = 0
            if not 1 <= interval <= 24:
                return fail("Интервал автопостинга — от 1 до 24 часов.")
        err = mp.precheck_core(u.id, text, selected_network, city)
        if err:
            import re as _re
            return fail(_re.sub(r'<[^>]+>', '', err))
        if u.id in _publishing:
            return fail("Публикация уже идёт…", 409)
        _publishing.add(u.id)
        try:
            res = mp.publish_core(u, text, media, selected_network, city)
        finally:
            _publishing.discard(u.id)
        auto = None
        if mode == "auto" and res["published"]:
            media_type, file_id = mp.media_kind(media)
            mp.autopost_queue.insert_one({
                "user_id": u.id, "network": selected_network, "city": city, "text": text,
                "media_type": media_type, "file_id": file_id, "media_array": media,
                "interval_hours": interval, "posts_left": 2,
                "next_run": mp.now_ekb() + mp.timedelta(hours=interval), "source": "cabinet"})
            auto = {"interval": interval, "left": 2}
        for p in res["published"]:
            cid = str(p["chat_id"]).replace("-100", "")
            p["link"] = f"https://t.me/c/{cid}/{p['message_id']}"
            p.pop("chat_id", None)
        if d.get("save_template") and res["published"]:
            _save_template(u.id, text, media, d.get("template_name"))
        return jsonify({"ok": True, "result": res, "auto": auto, "subs": subs_view(u.id)})

    @app.route('/cabinet/api/posts', methods=['POST'])
    def api_posts():
        u = auth()
        if not u: return unauthorized()
        rows = list(mp.ad_posts_collection.find({"user_id": u.id, "deleted": False}).sort("time", -1).limit(60))
        out = []
        for p in rows:
            cid = str(p.get("chat_id", "")).replace("-100", "")
            out.append({"id": str(p["_id"]), "network": p.get("network"), "city": p.get("city"),
                        "time": fmt_dt(p.get("time")), "link": f"https://t.me/c/{cid}/{p.get('message_id')}",
                        "auto": p.get("user_name") == "Автопост"})
        autos = []
        for t in mp.autopost_queue.find({"user_id": u.id}).sort("next_run", 1):
            autos.append({"id": str(t["_id"]), "network": t.get("network"), "city": t.get("city"),
                          "next": fmt_dt(t.get("next_run")), "left": t.get("posts_left", 0),
                          "interval": t.get("interval_hours", 0), "text": (t.get("text") or "")[:160]})
        return jsonify({"ok": True, "posts": out, "autoposts": autos})

    def _delete_post(p, who="Юзер (кабинет)"):
        try: bot.delete_message(p["chat_id"], p["message_id"])
        except Exception: pass
        for m_id in p.get("media_message_ids") or []:
            try: bot.delete_message(p["chat_id"], m_id)
            except Exception: pass
        mp.ad_posts_collection.update_one({"_id": p["_id"]}, {"$set": {"deleted": True, "deleted_by": who}})

    @app.route('/cabinet/api/posts/delete', methods=['POST'])
    def api_posts_delete():
        u = auth()
        if not u: return unauthorized()
        d = body()
        if d.get("all"):
            n = 0
            for p in mp.ad_posts_collection.find({"user_id": u.id, "deleted": False}):
                _delete_post(p); n += 1
            return jsonify({"ok": True, "deleted": n})
        try:
            p = mp.ad_posts_collection.find_one({"_id": ObjectId(str(d.get("id"))), "user_id": u.id, "deleted": False})
        except Exception:
            p = None
        if not p:
            return fail("Пост не найден или уже удалён.")
        _delete_post(p)
        return jsonify({"ok": True, "deleted": 1})

    @app.route('/cabinet/api/autoposts/cancel', methods=['POST'])
    def api_autopost_cancel():
        u = auth()
        if not u: return unauthorized()
        try:
            r = mp.autopost_queue.delete_one({"_id": ObjectId(str(body().get("id"))), "user_id": u.id})
        except Exception:
            return fail("Задача не найдена.")
        return jsonify({"ok": bool(r.deleted_count)})

    def _save_template(uid, text, media, name=None):
        name = (str(name or "").strip() or " ".join(text.split()[:3]) + "…")[:60]
        media_type, file_id = mp.media_kind(media)
        if mp.ad_templates_collection.count_documents({"user_id": uid}) >= 20:
            return None
        return mp.ad_templates_collection.insert_one({
            "user_id": uid, "name": name, "text": text, "media_type": media_type, "file_id": file_id,
            "media_array": media, "created_at": mp.now_ekb(), "source": "cabinet"}).inserted_id

    @app.route('/cabinet/api/templates', methods=['POST'])
    def api_templates():
        u = auth()
        if not u: return unauthorized()
        out = []
        for t in mp.ad_templates_collection.find({"user_id": u.id}).sort("created_at", -1).limit(30):
            media = t.get("media_array") or ([{"type": t["media_type"], "id": t["file_id"]}] if t.get("media_type") in ("photo", "video") and t.get("file_id") else [])
            out.append({"id": str(t["_id"]), "name": t.get("name") or "Без имени", "text": t.get("text", ""),
                        "media": media_out(u.id, media)})
        return jsonify({"ok": True, "templates": out})

    @app.route('/cabinet/api/templates/save', methods=['POST'])
    def api_templates_save():
        u = auth()
        if not u: return unauthorized()
        d = body()
        text = str(d.get("text") or "").strip()
        if not text:
            return fail("Пустой текст.")
        bad, word = mp.check_stop_words(text)
        if bad:
            return fail(f"Запрещённое слово: «{word}»")
        media = owns_media(u.id, d.get("media"))
        if media is None:
            return fail("Медиафайлы не найдены — загрузите их заново.")
        tid = _save_template(u.id, text[:3500], media, d.get("name"))
        if not tid:
            return fail("Максимум 20 шаблонов — удалите ненужные.")
        return jsonify({"ok": True, "id": str(tid)})

    @app.route('/cabinet/api/templates/delete', methods=['POST'])
    def api_templates_delete():
        u = auth()
        if not u: return unauthorized()
        try:
            mp.ad_templates_collection.delete_one({"_id": ObjectId(str(body().get("id"))), "user_id": u.id})
        except Exception:
            return fail("Шаблон не найден.")
        return jsonify({"ok": True})

    @app.route('/cabinet/api/channel_request', methods=['POST'])
    def api_channel_request():
        """Заявка на рекламу TG-канала/группы (как кнопка «📢 Реклама TG-групп» в боте)."""
        u = auth()
        if not u: return unauthorized()
        if mp.is_banned_in_network(u.id):
            return fail(mp.BANNED_TEXT.replace("\n", " "), 403)
        link = str(body().get("link") or "").strip()[:200]
        if len(link) < 4:
            return fail("Укажите ссылку на канал или @username.")
        if not throttle(("chan", u.id), 2, 3600):
            return fail("Заявка уже отправлена. Дождитесь решения администратора.", 429)
        markup = mp.types.InlineKeyboardMarkup()
        markup.add(mp.types.InlineKeyboardButton("✅ Одобрить", callback_data=f"vip_approve_{u.id}"),
                   mp.types.InlineKeyboardButton("❌ Отклонить", callback_data=f"vip_reject_{u.id}"))
        try:
            bot.send_message(mp.ADMIN_CHAT_ID,
                             f"🚨 <b>Новая заявка на рекламу канала (кабинет)</b>\n👤 <a href='tg://user?id={u.id}'>{mp.escape_html(u.first_name)}</a> (<code>{u.id}</code>)\n🔗 {mp.escape_html(link)}",
                             parse_mode="HTML", reply_markup=markup, message_thread_id=mp.ADS_THREAD_ID)
        except Exception as e:
            mp.log_error("кабинет: заявка на канал", e, u.id)
            return fail("Не удалось отправить заявку. Попробуйте позже.", 502)
        return jsonify({"ok": True})
