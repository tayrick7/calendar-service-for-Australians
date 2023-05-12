import sqlite3
from _datetime import datetime
import datetime
from flask import Flask, request, send_file
from flask_restx import Api, Resource, fields
import requests
import pandas as pd
from io import BytesIO
import matplotlib.pyplot as plt
from matplotlib.patches import Patch
import matplotlib
matplotlib.use('Agg')

app = Flask(__name__)
api = Api(app, version='1.0', title='My Calendar',
          description='A time-management and scheduling calendar service for Australians')

conn = sqlite3.connect('mydb.db', check_same_thread=False)
c = conn.cursor()

c.execute('''CREATE TABLE IF NOT EXISTS events
             (id INTEGER PRIMARY KEY AUTOINCREMENT,
             name TEXT NOT NULL,
             date TEXT NOT NULL,
             start_time TEXT NOT NULL,
             end_time TEXT NOT NULL,
             street TEXT NOT NULL,
             suburb TEXT NOT NULL,
             state TEXT NOT NULL,
             post_code TEXT NOT NULL,
             description TEXT,
             last_update TEXT);''')
conn.commit()

event = api.model('Event', {
    'name': fields.String(required=True, description='Event name'),
    'date': fields.String(required=True, description='Event date, format: dd-mm-yyyy'),
    'from': fields.String(required=True, description='Event start time, format: hh:mm'),
    'to': fields.String(required=True, description='Event end time, format: hh:mm'),
    'location': fields.Nested(api.model('Location', {
        'street': fields.String(required=True, description='Street name that event hold'),
        'suburb': fields.String(required=True, description='Official suburb name that event hold'),
        'state': fields.String(required=True,
                               description='State name that event hold, e.g New South Wales should be NSW'),
        'post-code': fields.String(required=True, description='Official postal code')
    }), required=True),
    'description': fields.String(description='Other information')
})


@api.route('/events')
class EventList(Resource):
    @api.doc('create_event')
    @api.response(201, 'Created')
    @api.response(200, 'Success')
    @api.response(400, 'Bad Request')
    @api.response(404, 'Resource Not Found')
    @api.response(500, 'Internal Server Error')
    @api.expect(event)
    def post(self):
        """Create a new event"""
        info = request.get_json()

        name = info['name']
        date_s = info['date']
        try:
            date_c = datetime.datetime.strptime(date_s, '%d-%m-%Y')
            date = datetime.datetime.strftime(date_c, '%d-%m-%Y')
        except ValueError:
            return {'message': 'Invalid date input. Please follow the format: DD-MM-YYYY.'}, 400

        try:
            start_time = datetime.datetime.strptime(info['from'], '%H:%M:%S').strftime('%H:%M')
        except ValueError:
            try:
                start_time = datetime.datetime.strptime(info['from'], '%H:%M').strftime('%H:%M')
            except ValueError:
                return {'message': 'Invalid time input. Please follow the format: HH:MM or HH:MM:SS.'}, 400

        try:
            end_time = datetime.datetime.strptime(info['to'], '%H:%M:%S').strftime('%H:%M')
        except ValueError:
            try:
                end_time = datetime.datetime.strptime(info['to'], '%H:%M').strftime('%H:%M')
            except ValueError:
                return {'message': 'Invalid time input. Please follow the format: HH:MM or HH:MM:SS.'}, 400

        street = info['location']['street']
        suburb = info['location']['suburb']
        state = info['location']['state']
        post_code = info['location']['post-code']
        description = info.get('description')

        q = "SELECT * FROM events WHERE date = ?"
        c.execute(q, (info['date'],))
        all_event = c.fetchall()
        for e in all_event:
            if (start_time < e[4] and end_time > e[3]) or (start_time < e[3] and end_time > e[4]) or (
                    start_time >= e[3] and start_time < e[4]) or (end_time > e[3] and end_time <= e[4]):
                return {'message': 'The event overlaps with another event'}, 400

        last_update = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        q = f"INSERT INTO events (name, date, start_time, end_time, street, suburb, state, post_code, description, last_update) VALUES ('{name}', '{date}', '{start_time}', '{end_time}', '{street}', '{suburb}', '{state}', '{post_code}', '{description}', '{last_update}')"
        c.execute(q)

        conn.commit()
        event_id = c.lastrowid
        response = {
            'id': event_id,
            'last-update': last_update,
            '_links': {
                'self': {'href': f'/events/{event_id}'}
            }
        }
        return response, 201

    @api.doc('Get_all_avaliable_events')
    @api.response(201, 'Created')
    @api.response(200, 'Success')
    @api.response(400, 'Bad Request')
    @api.response(404, 'Resource Not Found')
    @api.response(500, 'Internal Server Error')
    @api.param('order', 'Method to sort events (default: +id)')
    @api.param('page', 'Page number that shows in user interface (default: 1)')
    @api.param('size', 'Number of events per page (default: 10)')
    @api.param('filter', 'Comma separated value that what user want to know for each event (default: id,name)')
    def get(self):
        """Get all available events"""
        c.execute("SELECT * FROM events")
        r = c.fetchall()
        event_l = []
        for e in r:
            single_event = {
                "id": e[0],
                "name": e[1],
                "date": e[2],
                "from": e[3],
                "to": e[4],
                "street": e[5],
                "suburb": e[6],
                "state": e[7],
                "post_code": e[8],
                "description": e[9],
                "last_update": e[10]
            }

            event_l.append(single_event)
        try:
            order = request.args.get("order", "+id")
            n_p = int(request.args.get("page", 1))
            size = int(request.args.get("size", 10))
            condi = request.args.get("filter", "id,name")
            condition = condi.split(',')
        except (ValueError):
            return {"message": "Invalid Input"}, 400
        except (KeyError):
            return {"message": "Invalid Input"}, 400

        try:
            filtered_events = []
            for data in event_l:
                filtered_data = {}
                for field in condition:
                    filtered_data[field] = data[field]
                filtered_events.append(filtered_data)
        except KeyError:
            return {"message": "Invalid filter input, may contains space, symbol etc. Please follow the format that it is comma sperated and with no space."}, 400

        order_l = order.split(",")
        base = []
        for key in order_l:
            if key.startswith("-"):
                base.append((key[1:], True))
            elif key.startswith("+"):
                base.append((key[1:], False))

        sorted_events = sorted(filtered_events, key=lambda event: [event.get(key, None) for key, reverse in base],
                               reverse=base[-1][1])

        begin = (n_p - 1) * size
        last = begin + size
        show = sorted_events[begin:last]
        next = None
        previous = None
        if last < len(sorted_events):
            next = f"{request.base_url}?order={order}&page={n_p + 1}&size={size}&filter={condi}"
        if begin > 1:
            previous = f"{request.base_url}?order={order}&page={n_p - 1}&size={size}&filter={condi}"
        links = {"self": {"href": request.full_path}}
        if previous:
            links["previous"] = {"href": previous}
        if next:
            links["next"] = {"href": next}
        return {
                   "page": n_p,
                   "page-size": size,
                   "events": show,
                   "_links": links,
               }, 200


@api.route('/events/<int:event_id>')
@api.param('event_id', 'The ID of the event')
class Event(Resource):
    @api.doc('get_event')
    @api.response(201, 'Created')
    @api.response(200, 'Success')
    @api.response(400, 'Bad Request')
    @api.response(404, 'Resource Not Found')
    @api.response(500, 'Internal Server Error')
    def get(self, event_id):
        """Get an event by ID"""
        c.execute("SELECT * FROM events WHERE id = ?", (event_id,))
        row = c.fetchone()
        if row is None:
            return {'message': 'Event not found'}, 404
        aus_holidays = requests.get("https://date.nager.at/api/v2/publicholidays/2023/AU").json()
        holi_d = None
        event_date = datetime.datetime.strptime(row[2], '%d-%m-%Y')
        for e in aus_holidays:
            h_d = datetime.datetime.strptime(e['date'], '%Y-%m-%d')
            if h_d == event_date:
                holi_d = e['name']
                break

        previous_event = None
        next_event = None
        c.execute("SELECT * FROM events ")
        events = c.fetchall()
        for e in events:
            new_e = datetime.datetime.strptime(e[2], '%d-%m-%Y')
            if next_event:
                next_event = list(next_event)
                if isinstance(next_event[2], str):
                    next_event[2] = datetime.datetime.strptime(next_event[2], '%d-%m-%Y')
            if previous_event:
                previous_event = list(previous_event)
                if isinstance(previous_event[2], str):
                    previous_event[2] = datetime.datetime.strptime(previous_event[2], '%d-%m-%Y')
            if (not previous_event or new_e > previous_event[2] or
                (new_e == previous_event[2] and e[3] > previous_event[3])) and new_e < event_date or (
                    new_e == event_date and e[3] < row[3]):
                previous_event = e
            if (not next_event or new_e < next_event[2] or
                (new_e == next_event[2] and e[3] < next_event[3])) and new_e > event_date or (
                    new_e == event_date and e[3] > row[3]):
                next_event = e

        weather_forecast = {
            "wind-speed": None,
            "weather": None,
            "humidity": None,
            "temperature": None
        }
        current_date = datetime.date.today()
        difference = (event_date.date() - current_date).days
        if difference >= 1 and difference <= 7:
            try:
                filtered_df = sub[
                    (sub['Official Name Suburb'].str.contains(row[6])) & (sub['state'].str.contains(row[7]))]
                lat, lng = filtered_df['Geo Point'].iloc[0].split(', ')
                response = requests.get(
                    f"https://www.7timer.info/bin/civil.php?lat={lat}&lng={lng}&ac=1&unit=metric&output=json&product=two")
                weather = response.json()['dataseries'][0]
                weather_forecast = {
                    "wind-speed": f"{weather['wind10m']}{' The speed unit is KM.'}",
                    "weather": f"{weather['weather']}",
                    "humidity": f"{weather['rh2m']}",
                    "temperature": f"{weather['temp2m']}C"
                }
            except IndexError:
                pass
        links = {"self": {"href": f"/events/{row[0]}"}}
        if previous_event:
            links["previous"] = {"href": f"/events/{previous_event[0]}"}
        if next_event:
            links["next"] = {"href": f"/events/{next_event[0]}"}

        res = {
            'id': row[0],
            "last-update": row[10],
            'name': row[1],
            'date': row[2],
            'from': row[3],
            'to': row[4],
            'location': {
                'street': row[5],
                'suburb': row[6],
                'state': row[7],
                'post-code': row[8]
            },
            'description': row[9],
            "_metadata": {
                "wind-speed": None,
                "weather": None,
                "humidity": None,
                "temperature": None,
                "holiday": holi_d,
                "weekend": event_date.weekday() >= 5
            },
            "_links": links
        }

        if difference >= 1 and difference <= 7:
            res["_metadata"].update({
                "wind-speed": weather_forecast['wind-speed'],
                "weather": weather_forecast['weather'],
                "humidity": weather_forecast['humidity'],
                "temperature": weather_forecast['temperature'],
            })
        weather_vali = any(res["_metadata"].get(k) is not None for k in ["wind-speed", "weather", "humidity", "temperature"])
        if not weather_vali:
            res["_metadata"] = {"holiday": holi_d, "weekend": event_date.weekday() >= 5}
        return res, 200

    @api.doc('delete_event')
    @api.response(201, 'Created')
    @api.response(200, 'Success')
    @api.response(400, 'Bad Request')
    @api.response(404, 'Resource Not Found')
    @api.response(500, 'Internal Server Error')
    def delete(self, event_id):
        """Delete an event by ID"""
        c.execute("SELECT * FROM events WHERE id=%s" % event_id)
        row = c.fetchone()
        if row is None:
            return {'message': 'Event not exist'}, 404
        else:
            c.execute("DELETE FROM events WHERE id=?", (event_id,))
            conn.commit()
            return {'message': f'The event with id {event_id} was removed from the database!', 'id': event_id}, 200

    @api.doc('update an event by id')
    @api.expect(api.model('UpdateEvent', {
        'detail_of_event': fields.String('new_information', description='format of updating event')}))
    @api.response(201, 'Created')
    @api.response(200, 'Success')
    @api.response(400, 'Bad Request')
    @api.response(404, 'Resource Not Found')
    @api.response(500, 'Internal Server Error')
    def patch(self, event_id):
        """Update an event by ID"""
        data = request.get_json()
        if not data:
            return {'message': 'Invalid Input'}, 400

        c.execute("SELECT * FROM events WHERE id=%s" % event_id)
        row = c.fetchone()
        if row is None:
            return {'message': 'Event not found'}, 404
        else:
            name = row[1]
            date = row[2]
            start_time = row[3]
            end_time = row[4]
            street = row[5]
            suburb = row[6]
            state = row[7]
            post_code = row[8]
            description = row[9]
            if 'name' in data:
                name = data['name']
            if 'date' in data:
                date = data['date']
            if 'from' in data:
                start_time = data['from']
            if 'to' in data:
                end_time = data['to']
            if 'street' in data:
                street = data['street']
            if 'suburb' in data:
                suburb = data['suburb']
            if 'state' in data:
                state = data['state']
            if 'post_code' in data:
                post_code = data['post_code']
            if 'description' in data:
                description = data['description']

            query = "UPDATE events SET name='%s', date='%s', start_time='%s', end_time='%s', street='%s', suburb='%s', state='%s', post_code='%s', description='%s', last_update=CURRENT_TIMESTAMP WHERE id=%s" % (
                name, date, start_time, end_time, street, suburb, state, post_code, description, event_id)
            c.execute(query)

            conn.commit()

            res = {
                'id': event_id,
                'last_update': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                '_links': {
                    'self': {'href': f'/events/{event_id}'}
                }
            }
            return res, 200


@api.route('/events/statistics')
class Stats(Resource):
    @api.doc('Get_statistics_of_events')
    @api.response(201, 'Created')
    @api.response(200, 'Success')
    @api.response(400, 'Bad Request')
    @api.response(404, 'Resource Not Found')
    @api.response(500, 'Internal Server Error')
    @api.param('format', 'Response format (json or image)')
    def get(self):
        """Get statistics of existing events"""
        f_t = request.args.get("format", "json")
        c.execute("SELECT date FROM events")
        results = c.fetchall()
        total_events = len(results)
        stats = {}
        for row in results:
            date = row[0]
            stats[date] = stats[date] + 1 if date in stats else 1

        today = datetime.date.today()
        sw = today - datetime.timedelta(days=today.weekday())
        ew = sw + datetime.timedelta(days=6)
        sm = today.replace(day=1)
        em = sm.replace(month=sm.month % 12 + 1) - datetime.timedelta(days=1)
        cwe = sum(stats.get(date, 0) for date in stats if sw <= datetime.datetime.strptime(date, '%d-%m-%Y').date() <= ew and today.year == datetime.datetime.strptime(date, '%d-%m-%Y').date().year)
        cme = sum(stats.get(date, 0) for date in stats if sm <= datetime.datetime.strptime(date, '%d-%m-%Y').date() <= em and today.year == datetime.datetime.strptime(date, '%d-%m-%Y').date().year)

        if f_t == "image":
            x = list(stats.keys())
            x = [datetime.datetime.strptime(date, '%d-%m-%Y').date() for date in x]
            y = list(stats.values())
            color_choice = []
            u_c = set()
            for date in stats:
                if sw <= datetime.datetime.strptime(date, '%d-%m-%Y').date() <= ew \
                        and sm <= datetime.datetime.strptime(date, '%d-%m-%Y').date() <= em:
                    color = 'yellow'  # current week and month
                elif sw <= datetime.datetime.strptime(date, '%d-%m-%Y').date() <= ew:
                    color = 'blue'  # current week
                elif sm <= datetime.datetime.strptime(date, '%d-%m-%Y').date() <= em:
                    color = 'green'  # current month
                else:
                    color = 'grey'  # default color
                color_choice.append(color)
                if color not in u_c:
                    u_c.add(color)

            x, y, colors = zip(*sorted(zip(x, y, color_choice)))
            fig, ax = plt.subplots(figsize=(12, 6))
            ax.bar([date.strftime('%d-%m-%Y') for date in x], y, color=colors)
            ax.set_xlabel('Date')
            ax.set_ylabel('Number of events per day')
            ax.set_title('Event Statistics')
            ax.tick_params(axis='x', rotation=45, labelsize=8)

            layout = []
            for color in u_c:
                if color == 'green':
                    label = 'Current Month'
                elif color == 'yellow':
                    label = 'Current Week & Month'
                elif color == 'grey':
                    label = 'others'
                elif color == 'blue':
                    label = 'Current Week'
                layout.append(Patch(facecolor=color, label=label))

            ax.legend(handles=layout, loc='best')
            fig.tight_layout()
            img_buf = BytesIO()
            fig.savefig(img_buf, format='png')
            img_buf.seek(0)
            return send_file(img_buf, mimetype='image/png')

        return {
            "total": total_events,
            "total-current-week": cwe,
            "total-current-month": cme,
            "per-days": {k: v for k, v in sorted(stats.items(), key=lambda x: datetime.datetime.strptime(x[0], '%d-%m-%Y'), reverse=False)}
        }, 200


if __name__ == '__main__':
    sub = pd.read_csv('georef-australia-state-suburb.csv', delimiter=';')
    state_map = {
        'New South Wales': 'NSW',
        'Victoria': 'VIC',
        'Queensland': 'QLD',
        'Western Australia': 'WA',
        'South Australia': 'SA',
        'Tasmania': 'TAS',
        'Northern Territory': 'NT',
        'Australian Capital Territory': 'ACT'
    }
    sub['state'] = sub['Official Name State'].replace(state_map)
    app.run(debug=True)
